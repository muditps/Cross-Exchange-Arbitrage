package com.arbitrage.connector.kucoin;

import com.arbitrage.connector.ExchangeConnectionException;
import com.arbitrage.common.model.ExchangeId;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;

/**
 * Obtains and caches the WebSocket connection details from KuCoin's bullet-public endpoint.
 *
 * <p>KuCoin requires a two-step connection process unlike Binance or Bybit:
 * <ol>
 *   <li>POST to {@code /api/v1/bullet-public} to get a short-lived token and WS endpoint URL</li>
 *   <li>Open a WebSocket to {@code {endpoint}?token={TOKEN}&connectId={UUID}}</li>
 * </ol>
 *
 * <p><b>Why this service exists:</b> The REST call is separated into its own class for two
 * reasons. First, it isolates the HTTP concern from the WebSocket concern — {@link KuCoinConnector}
 * handles only WebSocket lifecycle. Second, it makes the REST call independently testable
 * without spinning up a WebSocket server.</p>
 *
 * <p><b>Caching strategy:</b> The token is cached as a {@code Mono<KuCoinConnectionDetails>}
 * with {@link Mono#cache()}. This means the REST call is made exactly once and all callers
 * share the same result. The cache is invalidated (set to {@code null}) in two situations:
 * <ul>
 *   <li>On any REST call failure — the next call will retry</li>
 *   <li>Explicitly via {@link #invalidateCache()} — called by {@link KuCoinConnector}
 *       before each reconnection attempt to ensure a fresh token</li>
 * </ul>
 *
 * <p>Public KuCoin tokens are valid for ~24 hours. Invalidating on reconnect is conservative
 * but safe — a token fetched 23h59m ago would cause an immediate auth failure, so proactively
 * refreshing on reconnect eliminates an entire class of hard-to-diagnose failures.</p>
 *
 * <p><b>Thread safety:</b> {@code cachedMono} is written under {@code synchronized} on both
 * {@link #getConnectionDetails()} and {@link #invalidateCache()}. This prevents two threads
 * from concurrently triggering duplicate REST calls at startup.</p>
 *
 * @see KuCoinConnectionDetails for the data returned by this service
 * @see KuCoinConnector for how connection details are used to build the WS URL
 */
@Component
@Slf4j
public class KuCoinTokenService {

    /**
     * KuCoin's public bullet endpoint. No API key required.
     * Returns a WebSocket token valid for ~24 hours and the WS endpoint URL.
     */
    static final String BULLET_PUBLIC_URL = "https://api.kucoin.com/api/v1/bullet-public";

    private final WebClient webClient;

    /**
     * The URL to POST to for obtaining a WebSocket token.
     * Defaults to {@link #BULLET_PUBLIC_URL}. Overridden in tests to point at a mock server.
     */
    private final String bulletPublicUrl;

    /**
     * Cached Mono holding the connection details. {@code null} means the cache has been
     * invalidated and the next call to {@link #getConnectionDetails()} will re-fetch.
     */
    private volatile Mono<KuCoinConnectionDetails> cachedMono = null;

    /**
     * Creates a KuCoinTokenService using Spring's auto-configured WebClient builder.
     *
     * <p>The builder is used rather than a pre-built WebClient because Spring Boot
     * auto-configuration applies sensible defaults (codecs, metrics, etc.) via the builder.</p>
     *
     * @param webClientBuilder Spring's auto-configured WebClient builder
     */
    @Autowired
    public KuCoinTokenService(WebClient.Builder webClientBuilder) {
        // Force HTTP/1.1 — KuCoin's Cloudflare layer resets HTTP/2 (ALPN) connections from
        // Reactor-Netty's default TLS ClientHello. curl uses HTTP/1.1 and connects successfully;
        // switching the protocol here matches that behavior.
        HttpClient httpClient = HttpClient.create().protocol(HttpProtocol.HTTP11);
        this.webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
        this.bulletPublicUrl = BULLET_PUBLIC_URL;
    }

    /**
     * Package-private constructor for testing — allows injecting a pre-configured WebClient
     * and a custom URL pointing at a mock HTTP server.
     *
     * @param webClient       a WebClient (no base URL required — the full URL is in bulletPublicUrl)
     * @param bulletPublicUrl the full URL to POST to (e.g., {@code http://localhost:PORT/api/v1/bullet-public})
     */
    KuCoinTokenService(WebClient webClient, String bulletPublicUrl) {
        this.webClient = webClient;
        this.bulletPublicUrl = bulletPublicUrl;
    }

    /**
     * Returns the KuCoin WebSocket connection details, fetching from the REST API if not cached.
     *
     * <p>On first call (or after cache invalidation), performs a POST to
     * {@link #BULLET_PUBLIC_URL} and caches the result as a shared {@code Mono}.
     * Subsequent calls return the cached Mono instantly without hitting the network.</p>
     *
     * <p>If the REST call fails, the cache is cleared so the next caller can retry rather
     * than receiving a permanently failed Mono.</p>
     *
     * @return a Mono emitting {@link KuCoinConnectionDetails}, or error on API failure
     */
    public synchronized Mono<KuCoinConnectionDetails> getConnectionDetails() {
        if (cachedMono == null) {
            log.info("Fetching KuCoin WebSocket token from bullet-public endpoint");
            cachedMono = fetchConnectionDetails()
                    .doOnError(error -> {
                        log.error("Failed to fetch KuCoin WebSocket token: {}", error.getMessage());
                        invalidateCache();
                    })
                    .cache();
        }
        return cachedMono;
    }

    /**
     * Invalidates the cached connection details.
     *
     * <p>Called by {@link KuCoinConnector} before each reconnection attempt to ensure
     * a fresh token is obtained. This prevents using a stale token (e.g., one fetched
     * close to its 24-hour expiry) after a long-running connection drop.</p>
     */
    public synchronized void invalidateCache() {
        log.debug("KuCoin token cache invalidated — will fetch fresh token on next connection");
        cachedMono = null;
    }

    /**
     * Performs the REST call to the bullet-public endpoint and extracts connection details.
     *
     * <p>The response is parsed from {@link KuCoinBulletPublicResponse} and the first
     * instance server is selected. If the response is unsuccessful or contains no servers,
     * an {@link ExchangeConnectionException} is emitted.</p>
     *
     * @return a Mono emitting the extracted connection details
     */
    private Mono<KuCoinConnectionDetails> fetchConnectionDetails() {
        return webClient.post()
                .uri(bulletPublicUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(KuCoinBulletPublicResponse.class)
                .flatMap(response -> {
                    if (!response.isSuccess() || response.getData() == null) {
                        return Mono.error(new ExchangeConnectionException(ExchangeId.KUCOIN,
                                "KuCoin bullet-public returned non-success code: " + response.getCode()));
                    }

                    KuCoinBulletPublicResponse.BulletData data = response.getData();
                    KuCoinBulletPublicResponse.InstanceServer server = data.getPrimaryServer();

                    if (server == null) {
                        return Mono.error(new ExchangeConnectionException(ExchangeId.KUCOIN,
                                "KuCoin bullet-public returned no instance servers"));
                    }

                    String token = data.getToken();
                    String wsEndpoint = server.getEndpoint();
                    long pingIntervalMs = server.getPingInterval();

                    log.info("KuCoin WebSocket token obtained: endpoint={} pingIntervalMs={}",
                            wsEndpoint, pingIntervalMs);

                    return Mono.just(new KuCoinConnectionDetails(token, wsEndpoint, pingIntervalMs));
                })
                .onErrorMap(ex -> !(ex instanceof ExchangeConnectionException),
                        ex -> new ExchangeConnectionException(ExchangeId.KUCOIN,
                                "REST call to bullet-public failed: " + ex.getMessage(), ex));
    }
}
