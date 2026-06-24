package com.arbitrage.connector;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

/**
 * Spring {@link ConfigurationProperties} for exchange connector configuration.
 *
 * <p>Binds per-exchange settings from {@code application.yml} under the
 * {@code arbitrage.exchanges} prefix. Each exchange is a keyed entry in the
 * {@code connectors} map, allowing N exchanges to be configured without code changes.</p>
 *
 * <p><b>Example YAML:</b></p>
 * <pre>{@code
 * arbitrage:
 *   exchanges:
 *     connectors:
 *       binance:
 *         ws-endpoint: wss://stream.binance.com:9443/ws
 *         taker-fee-rate: 0.0010
 *         staleness-threshold: 500ms
 *         enabled: true
 *       bybit:
 *         ws-endpoint: wss://stream.bybit.com/v5/public/spot
 *         taker-fee-rate: 0.0010
 *         enabled: false
 * }</pre>
 *
 * <p>Uses {@link Validated} with Jakarta Bean Validation constraints to fail fast
 * on startup if required fields are missing or invalid. This prevents silent
 * misconfiguration that could cause runtime failures during market hours.</p>
 *
 * @see ExchangeProperties for per-exchange settings
 */
@ConfigurationProperties(prefix = "arbitrage.exchanges")
@Validated
@Getter
@Setter
public class ExchangeConnectorProperties {

    /**
     * Per-exchange configuration map. Keys are exchange identifiers in lowercase
     * (e.g., "binance", "bybit", "kucoin"), matching {@link com.arbitrage.common.model.ExchangeId}
     * enum names in lowercase.
     */
    @NotEmpty(message = "At least one exchange connector must be configured")
    @Valid
    private Map<String, ExchangeProperties> connectors;

    /**
     * Configuration properties for a single exchange connector.
     *
     * <p>Contains connection settings (endpoint, reconnection), fee configuration,
     * and health monitoring thresholds. Uses {@link Duration} types for time-based
     * fields — Spring Boot auto-converts YAML values like "500ms", "1s", "30s"
     * into Duration objects, making configuration self-documenting.</p>
     *
     * <p>Uses {@code @Getter}/{@code @Setter} (not {@code @Value}) because Spring's
     * {@code @ConfigurationProperties} binding requires mutable POJO-style setters.</p>
     */
    @Getter
    @Setter
    public static class ExchangeProperties {

        /**
         * WebSocket endpoint URL for the exchange's market data feed.
         * Example: {@code wss://stream.binance.com:9443/ws}
         */
        @NotNull(message = "WebSocket endpoint URL is required")
        private String wsEndpoint;

        /**
         * Taker fee rate as a decimal fraction. For example, 0.10% is represented
         * as {@code 0.0010}. This overrides the default fee rate defined in
         * {@link com.arbitrage.common.model.ExchangeId} when the user has a
         * different fee tier (e.g., VIP discount).
         */
        @NotNull(message = "Taker fee rate is required")
        private BigDecimal takerFeeRate;

        /**
         * Initial delay before the first reconnection attempt after a connection drop.
         * Subsequent attempts use exponential backoff up to {@link #maxReconnectDelay}.
         * Defaults to 1 second.
         */
        @NotNull
        private Duration initialReconnectDelay = Duration.ofSeconds(1);

        /**
         * Maximum delay between reconnection attempts. This caps the exponential
         * backoff to prevent unreasonably long waits. Defaults to 30 seconds.
         */
        @NotNull
        private Duration maxReconnectDelay = Duration.ofSeconds(30);

        /**
         * Maximum number of consecutive reconnection attempts before the connector
         * gives up and transitions to {@link com.arbitrage.common.model.FeedStatus#DISCONNECTED}.
         * Defaults to 10 attempts.
         */
        @Positive(message = "Max reconnect attempts must be positive")
        private int maxReconnectAttempts = 10;

        /**
         * Duration after which a feed with no incoming messages is considered stale.
         * When elapsed, the connector transitions from
         * {@link com.arbitrage.common.model.FeedStatus#CONNECTED} to
         * {@link com.arbitrage.common.model.FeedStatus#STALE}. The detection engine
         * skips comparisons involving stale data to avoid false positives.
         * Defaults to 500 milliseconds.
         */
        @NotNull
        private Duration stalenessThreshold = Duration.ofMillis(500);

        /**
         * Whether this exchange connector is enabled. Allows disabling an exchange
         * without removing its configuration — useful during phased rollout
         * (e.g., only Binance enabled in Phase 1, Bybit/KuCoin added later).
         */
        private boolean enabled = true;
    }
}
