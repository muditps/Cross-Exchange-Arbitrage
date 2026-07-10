package com.arbitrage.connector.nse;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Map;

/**
 * Spring {@link ConfigurationProperties} for NSE-specific connector settings.
 *
 * <p>Bound from {@code arbitrage.nse.*} in {@code application.yml}. These settings
 * are separate from the generic {@code arbitrage.exchanges.connectors.nse} block because
 * they are equity-specific (auth credentials, TOTP, instrument token mapping) and do not
 * fit the generic {@link com.arbitrage.connector.ExchangeConnectorProperties} model shared
 * by crypto connectors.</p>
 *
 * <p><b>Instrument tokens:</b> Angel One SmartAPI identifies instruments by numeric token
 * (e.g., RELIANCE = 2885, TCS = 11536) rather than ticker symbols. The {@link #instruments}
 * map translates canonical pair symbols ({@code RELIANCE-INR}) to their Angel One tokens.
 * Adding a new equity pair requires only a new YAML entry — no code changes.</p>
 *
 * <p><b>TOTP:</b> Angel One enforces TOTP (Time-based One-Time Password, RFC 6238) on every
 * login. {@link #totpSecret} is the base32 secret from the authenticator app setup. When
 * provided, {@link NseTokenService} generates the current 6-digit code programmatically using
 * pure Java crypto (no external library), eliminating the need for manual code entry.</p>
 *
 * <p><b>Security:</b> All sensitive fields ({@code apiKey}, {@code clientCode}, {@code mpin},
 * {@code totpSecret}) are sourced exclusively from environment variables. They are never
 * logged, never hardcoded, and never committed. When the connector is disabled
 * ({@code NSE_ENABLED=false}), these fields may be left blank — the token service is never
 * called and Spring does not validate them at startup.</p>
 *
 * @see NseTokenService for how these properties are used to authenticate
 * @see NseConnector for how the instrument map drives WebSocket subscriptions
 */
@ConfigurationProperties(prefix = "arbitrage.nse")
@Validated
@Getter
@Setter
public class NseConnectorProperties {

    /**
     * Angel One developer API key. Obtained from smartapi.angelbroking.com after
     * registering an application. Passed in the {@code X-PrivateKey} REST header.
     * Required when NSE connector is enabled; may be blank otherwise.
     */
    private String apiKey = "";

    /**
     * Angel One client code (account user ID). Shown on the Angel One login screen.
     * Also used as the WebSocket {@code x-client-code} header on the SmartStream feed.
     */
    private String clientCode = "";

    /**
     * Angel One MPIN (market PIN, 4–6 digits). Used alongside {@link #totpSecret}
     * to authenticate via the login REST endpoint.
     */
    private String mpin = "";

    /**
     * Base32-encoded TOTP secret from the authenticator app setup page on Angel One.
     * {@link NseTokenService} generates the current 6-digit code from this secret using
     * HMAC-SHA1 per RFC 6238. Rotate immediately if accidentally exposed.
     */
    private String totpSecret = "";

    /**
     * URL for the Angel One SmartAPI login endpoint.
     * Defaults to the production endpoint; override in tests via
     * {@code arbitrage.nse.login-url: http://localhost:PORT/mock/login}.
     */
    private String loginUrl =
            "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword";

    /**
     * Interval in seconds between WebSocket heartbeat pings sent to Angel One's
     * SmartStream server. Angel One closes the connection if no ping is received
     * within ~60 seconds. Default: 30 seconds (half the timeout, with margin).
     */
    private int heartbeatIntervalSeconds = 30;

    /**
     * Mapping from canonical pair symbol to Angel One instrument token.
     *
     * <p>Key: canonical {@code BASE-INR} symbol (e.g., {@code RELIANCE-INR}).<br>
     * Value: Angel One NSE instrument token as a string (e.g., {@code "2885"}).</p>
     *
     * <p>Common tokens (NSE cash market):</p>
     * <ul>
     *   <li>RELIANCE (Reliance Industries) → 2885</li>
     *   <li>TCS (Tata Consultancy Services) → 11536</li>
     *   <li>INFY (Infosys) → 1594</li>
     *   <li>HDFCBANK (HDFC Bank) → 1333</li>
     *   <li>ICICIBANK (ICICI Bank) → 4963</li>
     * </ul>
     *
     * <p>Token numbers are stable and do not change unless the instrument is
     * delisted and re-listed. Source: Angel One instrument master file at
     * {@code https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json}</p>
     */
    private Map<String, String> instruments = Map.of();
}
