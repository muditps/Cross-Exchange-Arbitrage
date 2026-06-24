package com.arbitrage.detection.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized configuration for the Detection Engine.
 *
 * <p>All tunable parameters are bound from {@code application.yml} under the
 * {@code arbitrage.detection} prefix. Override via environment variables without
 * recompiling.
 *
 * <p>Registered as a Spring bean via
 * {@code @EnableConfigurationProperties(DetectionProperties.class)} in
 * {@link com.arbitrage.detection.DetectionEngineApplication}.
 *
 * <p><b>Two distinct time concepts:</b>
 * <ul>
 *   <li>{@link #redisPriceTtlMs} — how long a Redis price key lives before auto-eviction.
 *       Set deliberately longer than the staleness threshold so keys are not evicted
 *       before the engine has a chance to read and reject them on age grounds.
 *       Default: 10,000ms (10s).</li>
 *   <li>{@link #stalenessThresholdMs} — the maximum age a price may be at
 *       <em>comparison time</em>. Even if a key is still in Redis, a price older than
 *       this is excluded from spread comparisons to prevent phantom signals.
 *       Default: 500ms.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "arbitrage.detection")
@Getter
@Setter
public class DetectionProperties {

    /**
     * Maximum age of a price (milliseconds) before it is excluded from spread comparisons.
     *
     * <p>Stale prices cause false-positive arbitrage signals. For example, Binance at
     * T-200ms vs KuCoin at T-0ms may show a spread that has already closed. Prices older
     * than this threshold are skipped during comparison. Default: 500ms.
     */
    private long stalenessThresholdMs = 500;

    /**
     * Minimum spread in basis points (bps) before an opportunity is published.
     *
     * <p>1 bps = 0.01%. At 10 bps minimum, a $40,000 BTC must show at least $40 spread
     * before we publish an {@code ArbitrageOpportunity}. This threshold does NOT account
     * for fees — fee filtering happens downstream in the detection comparison logic.
     * Default: 10 bps.
     */
    private int minSpreadBps = 10;

    /**
     * Maximum duration (milliseconds) an opportunity may remain in the OPEN state.
     *
     * <p>An opportunity open for longer than this threshold likely indicates a data
     * artefact — a stale feed producing a persistent phantom spread rather than a
     * genuine market opportunity. When the background expiry sweep (every 5 seconds)
     * finds an OPEN entry older than this value, it transitions it to EXPIRED and
     * removes it from the tracker. Default: 60,000ms (60s).
     */
    private long maxOpportunityDurationMs = 60_000;

    /**
     * Redis TTL for price state keys (milliseconds).
     *
     * <p>Each {@code price:{exchange}:{pair}} hash key is given this TTL on every write.
     * When the TTL expires, Redis automatically evicts the key — no separate cleanup
     * sweep is required. A key that has expired is definitionally stale.
     *
     * <p>This value is intentionally larger than {@link #stalenessThresholdMs} (10s vs 500ms):
     * the 500ms threshold filters prices at comparison time; the 10s TTL is a last-resort
     * cleanup to prevent unbounded Redis growth if a connector silently stops producing.
     * Default: 10,000ms (10s).
     */
    private long redisPriceTtlMs = 10_000;
}
