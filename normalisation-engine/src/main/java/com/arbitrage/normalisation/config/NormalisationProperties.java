package com.arbitrage.normalisation.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized configuration for the Normalisation Engine.
 *
 * <p>Bound from {@code application.yml} under the {@code arbitrage.normalisation} prefix.
 * Override via environment variables without recompiling.
 */
@ConfigurationProperties(prefix = "arbitrage.normalisation")
@Getter
@Setter
public class NormalisationProperties {

    /**
     * Maximum age (milliseconds) of a raw tick before it is dropped without normalising.
     *
     * <p>Raw ticks accumulate in Kafka when the normalisation engine falls behind (backlog from
     * load tests, restarts, or burst traffic). Without this filter, the engine forwards stale
     * backlog messages to the detection engine, which then rejects them all as stale — wasting
     * CPU and blocking the pipeline from processing fresh ticks.
     *
     * <p>With this filter enabled, stale ticks are dropped in ~0.01ms (read + check + ack)
     * instead of ~1ms (full transform + Kafka publish). At 700 ticks/sec, a 100,000-message
     * backlog clears in ~1.4 seconds instead of ~143 seconds.
     *
     * <p>Set slightly higher than {@code arbitrage.detection.staleness-threshold-ms} to account
     * for normalisation → detection Kafka transit time. Default: 2000ms.
     */
    private long stalenessThresholdMs = 2000;

    /**
     * Number of consumer threads — one per raw-ticks topic (binance, bybit, kucoin).
     */
    private int consumerConcurrency = 3;
}
