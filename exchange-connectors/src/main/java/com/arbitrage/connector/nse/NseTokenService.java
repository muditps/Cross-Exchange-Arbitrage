package com.arbitrage.connector.nse;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.connector.ExchangeConnectionException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Obtains and caches Angel One SmartAPI authentication credentials for the NSE connector.
 *
 * <p><b>Angel One auth flow (two-step, unlike KuCoin's one-step token fetch):</b>
 * <ol>
 *   <li>Generate a fresh TOTP code from the configured base32 secret using HMAC-SHA1
 *       (RFC 6238, 30-second time step, 6-digit output).</li>
 *   <li>POST to the login endpoint with clientCode + mpin + totp in the JSON body and
 *       the API key in the {@code X-PrivateKey} header.</li>
 *   <li>Extract {@code jwtToken} and {@code feedToken} from the response.</li>
 *   <li>Return an {@link NseAuthCredentials} record containing both tokens + clientCode.</li>
 * </ol>
 *
 * <p><b>Caching strategy:</b> Credentials are cached as a {@code Mono<NseAuthCredentials>}
 * with {@link Mono#cache()}. The cache is invalidated (set to {@code null}) before each
 * reconnection attempt by {@link NseConnector} — the same pattern used by
 * {@link com.arbitrage.connector.kucoin.KuCoinTokenService}. Angel One JWTs are session-scoped
 * (valid for one trading day); proactive refresh on reconnect prevents stale-token failures.</p>
 *
 * <p><b>TOTP generation:</b> Implemented with pure Java ({@code javax.crypto.Mac}, no external
 * library). Steps: base32-decode the secret, compute HMAC-SHA1 over the current 30-second
 * time counter as an 8-byte big-endian long, apply dynamic truncation per RFC 4226 §5.4,
 * and reduce modulo 10^6 to produce a zero-padded 6-digit string. The code is generated
 * fresh on each login call — caching it would fail across 30-second boundaries.</p>
 *
 * <p><b>Thread safety:</b> {@code cachedMono} is written under {@code synchronized}. Two threads
 * racing to connect at startup will not issue duplicate login requests.</p>
 *
 * @see NseConnector for how these credentials are applied to WebSocket headers
 * @see NseConnectorProperties for the auth configuration fields
 */
@Component
@Slf4j
public class NseTokenService {

    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";
    private static final int TOTP_TIME_STEP_SECONDS = 30;
    private static final int TOTP_DIGITS = 6;

    private final WebClient webClient;
    private final NseConnectorProperties nseProps;

    /** Cached Mono; null means invalidated and the next call will re-authenticate. */
    private volatile Mono<NseAuthCredentials> cachedMono = null;

    /**
     * Creates the token service with Spring's auto-configured WebClient builder.
     *
     * @param webClientBuilder Spring's auto-configured WebClient builder
     * @param nseProps         NSE connector properties containing auth fields
     */
    @Autowired
    public NseTokenService(WebClient.Builder webClientBuilder, NseConnectorProperties nseProps) {
        this.webClient = webClientBuilder.build();
        this.nseProps = nseProps;
    }

    /**
     * Package-private constructor for testing — allows injecting a pre-configured WebClient
     * and custom properties pointing at a mock HTTP server.
     *
     * @param webClient a pre-configured WebClient (base URL optional — full URL used in call)
     * @param nseProps  properties containing the mock login URL and test credentials
     */
    NseTokenService(WebClient webClient, NseConnectorProperties nseProps) {
        this.webClient = webClient;
        this.nseProps = nseProps;
    }

    /**
     * Returns cached Angel One credentials, fetching from the login API if not cached.
     *
     * <p>On first call (or after cache invalidation), generates a fresh TOTP code and
     * POSTs to the Angel One login endpoint. The result is cached as a shared Mono.
     * Subsequent calls return instantly without network I/O.</p>
     *
     * <p>If the login call fails, the cache is cleared so the next caller can retry
     * rather than receiving a permanently failed Mono.</p>
     *
     * @return a Mono emitting {@link NseAuthCredentials}, or error on auth failure
     */
    public synchronized Mono<NseAuthCredentials> getCredentials() {
        if (cachedMono == null) {
            log.info("Authenticating with Angel One SmartAPI: clientCode={}", nseProps.getClientCode());
            cachedMono = fetchCredentials()
                    .doOnError(error -> {
                        log.error("Angel One authentication failed: {}", error.getMessage());
                        invalidateCache();
                    })
                    .cache();
        }
        return cachedMono;
    }

    /**
     * Invalidates the cached credentials.
     *
     * <p>Called by {@link NseConnector} before each reconnection attempt to ensure a
     * fresh JWT is obtained. Angel One tokens are session-scoped; a reconnect after a
     * long gap may use an expired token if the cache is not cleared.</p>
     */
    public synchronized void invalidateCache() {
        log.debug("NSE auth cache invalidated — will re-authenticate on next connection");
        cachedMono = null;
    }

    /**
     * Performs the Angel One login REST call and extracts auth credentials.
     *
     * <p>Generates a current TOTP code immediately before the call (not cached) to ensure
     * the code is valid within its 30-second window. A TOTP code generated 29 seconds ago
     * would be rejected by Angel One's server-side clock — generating fresh each time is safe.</p>
     *
     * @return a Mono emitting the extracted credentials
     */
    private Mono<NseAuthCredentials> fetchCredentials() {
        String totp;
        try {
            totp = generateTotp(nseProps.getTotpSecret());
        } catch (Exception e) {
            return Mono.error(new ExchangeConnectionException(ExchangeId.NSE,
                    "Failed to generate TOTP code: " + e.getMessage(), e));
        }

        Map<String, String> requestBody = Map.of(
                "clientcode", nseProps.getClientCode(),
                "password", nseProps.getMpin(),
                "totp", totp
        );

        return webClient.post()
                .uri(nseProps.getLoginUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-UserType", "USER")
                .header("X-SourceID", "WEB")
                .header("X-ClientLocalIP", "127.0.0.1")
                .header("X-ClientPublicIP", "127.0.0.1")
                .header("X-MACAddress", "00:00:00:00:00:00")
                .header("X-PrivateKey", nseProps.getApiKey())
                .bodyValue(requestBody)
                .retrieve()
                .bodyToMono(AngelOneLoginResponse.class)
                .flatMap(response -> {
                    if (!response.isStatus() || response.getData() == null) {
                        return Mono.error(new ExchangeConnectionException(ExchangeId.NSE,
                                "Angel One login failed: errorcode=" + response.getErrorcode()
                                        + " message=" + response.getMessage()));
                    }
                    AngelOneLoginResponse.LoginData data = response.getData();
                    if (data.getJwtToken() == null || data.getFeedToken() == null) {
                        return Mono.error(new ExchangeConnectionException(ExchangeId.NSE,
                                "Angel One login response missing jwtToken or feedToken"));
                    }
                    log.info("Angel One authentication successful: clientCode={}", nseProps.getClientCode());
                    return Mono.just(new NseAuthCredentials(
                            data.getJwtToken(),
                            data.getFeedToken(),
                            nseProps.getClientCode()));
                })
                .onErrorMap(ex -> !(ex instanceof ExchangeConnectionException),
                        ex -> new ExchangeConnectionException(ExchangeId.NSE,
                                "REST call to Angel One login endpoint failed: " + ex.getMessage(), ex));
    }

    /**
     * Generates a 6-digit TOTP code using HMAC-SHA1 per RFC 6238.
     *
     * <p><b>Algorithm:</b>
     * <ol>
     *   <li>Decode the base32 secret to raw bytes.</li>
     *   <li>Compute the current time counter: {@code floor(unixTimeSeconds / 30)}.</li>
     *   <li>HMAC-SHA1 the counter (as an 8-byte big-endian long) with the decoded secret.</li>
     *   <li>Dynamic truncation (RFC 4226 §5.4): extract a 4-byte slice starting at the
     *       offset indicated by the last nibble of the HMAC, mask the high bit, interpret
     *       as an unsigned int.</li>
     *   <li>Reduce modulo 10^6 and zero-pad to 6 digits.</li>
     * </ol>
     *
     * <p><b>Why pure Java:</b> TOTP is a compact algorithm with no dependencies beyond
     * {@code javax.crypto}. Introducing a library (e.g., googleauth) for ~20 lines of
     * standard crypto is unnecessary overhead and an additional supply-chain dependency.
     *
     * @param base32Secret the base32-encoded TOTP secret from the authenticator app setup
     * @return a zero-padded 6-digit TOTP string (e.g., "042871")
     * @throws NoSuchAlgorithmException if HmacSHA1 is unavailable (never on standard JVMs)
     * @throws InvalidKeyException      if the decoded key bytes are malformed
     */
    String generateTotp(String base32Secret) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] key = base32Decode(base32Secret);
        long timeCounter = System.currentTimeMillis() / 1000L / TOTP_TIME_STEP_SECONDS;
        byte[] counterBytes = ByteBuffer.allocate(8).putLong(timeCounter).array();

        Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
        mac.init(new SecretKeySpec(key, HMAC_SHA1_ALGORITHM));
        byte[] hmac = mac.doFinal(counterBytes);

        // Dynamic truncation: offset = last nibble of HMAC
        int offset = hmac[hmac.length - 1] & 0x0f;
        int truncated = ((hmac[offset] & 0x7f) << 24)
                | ((hmac[offset + 1] & 0xff) << 16)
                | ((hmac[offset + 2] & 0xff) << 8)
                | (hmac[offset + 3] & 0xff);

        int code = truncated % (int) Math.pow(10, TOTP_DIGITS);
        return String.format("%0" + TOTP_DIGITS + "d", code);
    }

    /**
     * Decodes a base32-encoded string to raw bytes.
     *
     * <p>Handles both padded ({@code =}) and unpadded base32. Case-insensitive.
     * Uses the standard RFC 4648 base32 alphabet ({@code A–Z}, {@code 2–7}).
     * Non-alphabet characters (spaces, hyphens from some authenticator exports) are stripped.</p>
     *
     * @param encoded the base32-encoded string
     * @return the decoded byte array
     */
    private static byte[] base32Decode(String encoded) {
        final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        String cleaned = encoded.toUpperCase().replaceAll("[^A-Z2-7]", "");
        int byteCount = cleaned.length() * 5 / 8;
        byte[] result = new byte[byteCount];
        int buffer = 0, bitsLeft = 0, index = 0;
        for (char c : cleaned.toCharArray()) {
            buffer = (buffer << 5) | alphabet.indexOf(c);
            bitsLeft += 5;
            if (bitsLeft >= 8) {
                result[index++] = (byte) (buffer >> (bitsLeft - 8));
                bitsLeft -= 8;
            }
        }
        return result;
    }

    /**
     * Wire-format POJO for the Angel One SmartAPI login response.
     * Fields are mapped from the JSON response body.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Getter
    static class AngelOneLoginResponse {

        /** {@code true} on successful login, {@code false} on failure. */
        private boolean status;

        /** Human-readable message from Angel One (e.g., "SUCCESS", "INVALID TOTP"). */
        private String message;

        /** Angel One error code string; empty string on success. */
        private String errorcode;

        /** Auth tokens; present only when {@link #status} is {@code true}. */
        private LoginData data;

        @JsonIgnoreProperties(ignoreUnknown = true)
        @Getter
        static class LoginData {

            /** Bearer JWT for REST calls and WebSocket Authorization header. */
            @JsonProperty("jwtToken")
            private String jwtToken;

            /** SmartStream-specific feed token for the x-feed-token WebSocket header. */
            @JsonProperty("feedToken")
            private String feedToken;

            /** Refresh token for extending the session (not used in connector flow). */
            @JsonProperty("refreshToken")
            private String refreshToken;
        }
    }
}
