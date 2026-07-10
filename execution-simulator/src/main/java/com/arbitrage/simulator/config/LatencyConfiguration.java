package com.arbitrage.simulator.config;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.simulator.model.ExchangeLatencyProfile;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Externalized latency configuration for all supported exchanges.
 *
 * <p>Values are bound from {@code application.yml} under
 * {@code arbitrage.simulation.latency}. Each exchange has a distinct
 * {@link ExchangeLatencyConfig} with four latency components:
 * network outbound, exchange processing, confirmation inbound, and jitter.
 *
 * <p>Registered via {@code @EnableConfigurationProperties(LatencyConfiguration.class)} in
 * {@link com.arbitrage.simulator.ExecutionSimulatorApplication}.
 *
 * <p><b>Why separate from the enum?</b> Latency figures vary by co-location status,
 * network provider, and exchange tier. YAML-backed properties let operators tune for
 * their specific deployment without recompilation — crucial for an HFT-adjacent system
 * where latency is a commercial differentiator.
 *
 * <p><b>Why not combine with FeeConfiguration?</b> Latency belongs to the simulation
 * module; fees belong to the detection module. Merging them would create cross-module
 * coupling via a shared config class.
 *
 * <p><b>Default profiles (non-co-located, realistic retail/institutional estimates):</b>
 * <ul>
 *   <li>Binance: 15 + 5 + 5 + 2 = 27ms total</li>
 *   <li>Bybit:   25 + 8 + 8 + 2 = 43ms total</li>
 *   <li>KuCoin:  30 + 10 + 10 + 2 = 52ms total</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "arbitrage.simulation.latency")
@Validated
@Getter
@Setter
public class LatencyConfiguration {

    @Valid
    @NotNull
    private ExchangeLatencyConfig binance = new ExchangeLatencyConfig(15, 5, 5, 2);

    @Valid
    @NotNull
    private ExchangeLatencyConfig bybit = new ExchangeLatencyConfig(25, 8, 8, 2);

    @Valid
    @NotNull
    private ExchangeLatencyConfig kucoin = new ExchangeLatencyConfig(30, 10, 10, 2);

    /**
     * NSE latency profile (via Angel One SmartAPI, non-co-located).
     * Network latency is lower than crypto exchanges (Mumbai data centre proximity).
     * NSE matching engine is one of the fastest in Asia (~0.1ms internal, but retail
     * access via broker adds ~3ms processing). Defaults are conservative estimates.
     */
    @Valid
    @NotNull
    private ExchangeLatencyConfig nse = new ExchangeLatencyConfig(10, 3, 3, 1);

    /** BSE latency profile — similar to NSE (same Mumbai geography, same broker API). */
    @Valid
    @NotNull
    private ExchangeLatencyConfig bse = new ExchangeLatencyConfig(10, 3, 3, 1);

    /**
     * Returns an immutable {@link ExchangeLatencyProfile} for the given exchange.
     *
     * <p>The profile is constructed from the current configuration values each call.
     * Not cached — this is only called during simulator construction, not on the hot path.
     *
     * @param exchangeId the exchange to look up
     * @return immutable latency profile for use in execution time simulation
     * @throws IllegalArgumentException if {@code exchangeId} is null or unmapped
     */
    public ExchangeLatencyProfile getProfile(ExchangeId exchangeId) {
        ExchangeLatencyConfig config = switch (exchangeId) {
            case BINANCE -> binance;
            case BYBIT   -> bybit;
            case KUCOIN  -> kucoin;
            case NSE     -> nse;
            case BSE     -> bse;
        };
        return ExchangeLatencyProfile.builder()
                .exchangeId(exchangeId)
                .networkLatencyMs(config.getNetworkLatencyMs())
                .exchangeProcessingMs(config.getExchangeProcessingMs())
                .confirmationLatencyMs(config.getConfirmationLatencyMs())
                .jitterMs(config.getJitterMs())
                .build();
    }

    /**
     * Per-exchange latency component configuration.
     *
     * <p>All values are in milliseconds and must be non-negative. Zero is valid
     * (a co-located system may model sub-millisecond network latency as 0ms).
     * Non-zero defaults represent a realistic non-co-located participant.
     */
    @Getter
    @Setter
    public static class ExchangeLatencyConfig {

        /** One-way outbound network latency in milliseconds. */
        @Min(value = 0)
        private long networkLatencyMs;

        /** Exchange matching engine processing time in milliseconds. */
        @Min(value = 0)
        private long exchangeProcessingMs;

        /** One-way inbound confirmation latency in milliseconds. */
        @Min(value = 0)
        private long confirmationLatencyMs;

        /** p99 jitter buffer in milliseconds. */
        @Min(value = 0)
        private long jitterMs;

        public ExchangeLatencyConfig() {}

        public ExchangeLatencyConfig(long networkLatencyMs, long exchangeProcessingMs,
                                     long confirmationLatencyMs, long jitterMs) {
            this.networkLatencyMs = networkLatencyMs;
            this.exchangeProcessingMs = exchangeProcessingMs;
            this.confirmationLatencyMs = confirmationLatencyMs;
            this.jitterMs = jitterMs;
        }
    }
}
