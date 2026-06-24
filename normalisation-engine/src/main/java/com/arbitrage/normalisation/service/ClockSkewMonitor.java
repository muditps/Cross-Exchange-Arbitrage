package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Tracks the rolling offset between each exchange's server clock and the local wall clock.
 *
 * <p><b>Why clock skew matters in trading:</b> Each exchange runs its own NTP-synced server
 * clock. Binance's clock, KuCoin's clock, and our local clock all drift independently —
 * sometimes by hundreds of milliseconds. If we compare prices using exchange timestamps
 * naively, we might be comparing Binance's "now" with KuCoin's "500ms ago." This creates
 * phantom arbitrage opportunities that don't actually exist.
 *
 * <p><b>Our approach — use local {@code receivedTimestamp} for staleness, not exchange
 * timestamps:</b> The {@code receivedTimestamp} (captured by {@code System.nanoTime()} at
 * WebSocket message arrival) is on our clock. Staleness comparisons within the detection
 * engine use only this field. Exchange timestamps are preserved for ordering within a single
 * exchange and for calculating skew here, but never used for cross-exchange comparisons.
 *
 * <p><b>What this monitor measures — offset, not absolute skew:</b>
 * <pre>
 *   offsetMs = localWallClockMs - exchangeTimestampMs
 * </pre>
 * Positive = exchange timestamp is in our past (expected: includes network + Kafka latency).
 * Negative = exchange clock is ahead of ours (unusual; suggests NTP desync on our side).
 *
 * <p>Because the offset includes network latency and Kafka transit time, it is NOT a pure
 * clock skew measurement. What matters is <em>stability</em> — the offset should be
 * roughly constant for a given exchange over time. A sudden jump of 500ms+ indicates
 * either the exchange's NTP resynchronised, or an unusual processing delay.
 *
 * <p><b>EWMA (Exponentially Weighted Moving Average):</b> The rolling mean uses EWMA
 * with configurable alpha (default 0.1). Low alpha = heavy smoothing, slow adaptation.
 * High alpha = fast adaptation, more sensitive to short spikes. At alpha=0.1:
 * <ul>
 *   <li>After 10 ticks from 0ms → 100ms offset: EWMA ≈ 65ms (not yet converged)</li>
 *   <li>After 50 ticks: EWMA ≈ 99ms (nearly converged)</li>
 * </ul>
 * This smoothing prevents transient network spikes from triggering false alerts.
 *
 * <p><b>Binance caveat:</b> Binance's bookTicker does not include a server timestamp.
 * The connector sets {@code exchangeTimestamp = Instant.now()} (local clock). The offset
 * for Binance will always be near 0ms, and no jump will ever be detected. This is expected
 * and harmless — the gauge still exists but will always read ~0.
 *
 * <p><b>Thread safety:</b> {@link ConcurrentHashMap#compute} is atomic per key — the EWMA
 * update for a given exchange is linearised even if two Kafka threads deliver ticks
 * from the same exchange concurrently (unlikely but possible).
 *
 * @see FeedHealthMonitor for feed liveness tracking (different concern)
 */
@Component
@Slf4j
public class ClockSkewMonitor {

    private static final String METRIC_OFFSET = "normalisation.clock.skew.offset";
    private static final String TAG_EXCHANGE = "exchange";

    /**
     * Rolling EWMA of {@code localMs - exchangeMs} per exchange.
     * Null entry = first tick not yet received for this exchange.
     */
    private final Map<ExchangeId, Double> rollingMeanOffsetMs = new ConcurrentHashMap<>();

    /**
     * Micrometer gauge backing values — stores the rounded rolling mean per exchange.
     * Lazily registered on first tick per exchange.
     */
    private final Map<ExchangeId, AtomicLong> offsetGauges = new ConcurrentHashMap<>();

    /**
     * Absolute deviation from EWMA that triggers a WARN log.
     * Default: 500ms — a sudden 500ms shift is clearly anomalous for exchange clocks.
     */
    private final long jumpThresholdMs;

    /**
     * EWMA smoothing factor α (alpha). Blends new observations:
     * {@code newMean = α × currentOffset + (1 - α) × previousMean}.
     * Low value (0.1) = slow to adapt, filters transient spikes.
     */
    private final double ewmaAlpha;

    /**
     * Wall-clock supplier returning epoch milliseconds. Injected for testability.
     * Production uses {@code System::currentTimeMillis}; tests use {@code AtomicLong::get}.
     */
    private final Supplier<Long> wallClockMillisSupplier;

    private final MeterRegistry meterRegistry;

    /**
     * Production constructor. Spring injects configuration from application properties.
     *
     * @param jumpThresholdMs deviation from rolling mean (in ms) that triggers a WARN
     * @param ewmaAlpha       EWMA smoothing factor (0 < alpha ≤ 1)
     * @param meterRegistry   Micrometer registry for offset gauges
     */
    @Autowired
    public ClockSkewMonitor(
            @Value("${arbitrage.normalisation.clock-skew.jump-threshold-ms:500}") long jumpThresholdMs,
            @Value("${arbitrage.normalisation.clock-skew.ewma-alpha:0.1}") double ewmaAlpha,
            MeterRegistry meterRegistry) {
        this(System::currentTimeMillis, jumpThresholdMs, ewmaAlpha, meterRegistry);
    }

    /**
     * Test constructor. Injects a controllable wall-clock for deterministic tests.
     *
     * <p>Package-private — not part of the public API. Allows tests to set the local
     * wall clock to any value without system clock dependency.
     *
     * @param wallClockMillisSupplier clock source returning epoch milliseconds
     * @param jumpThresholdMs         deviation threshold for WARN logging
     * @param ewmaAlpha               EWMA smoothing factor
     * @param meterRegistry           Micrometer registry for offset gauges
     */
    ClockSkewMonitor(
            Supplier<Long> wallClockMillisSupplier,
            long jumpThresholdMs,
            double ewmaAlpha,
            MeterRegistry meterRegistry) {
        this.wallClockMillisSupplier = wallClockMillisSupplier;
        this.jumpThresholdMs = jumpThresholdMs;
        this.ewmaAlpha = ewmaAlpha;
        this.meterRegistry = meterRegistry;
        log.info("ClockSkewMonitor initialised: jumpThresholdMs={} ewmaAlpha={}", jumpThresholdMs, ewmaAlpha);
    }

    // ========================================================================
    // Public API — called by NormalisationService
    // ========================================================================

    /**
     * Records a tick observation and updates the rolling offset for the given exchange.
     *
     * <p>Computes {@code offsetMs = localWallClockMs - exchangeTimestampMs}, updates the
     * EWMA, and logs a WARN if the deviation from the rolling mean exceeds
     * {@link #jumpThresholdMs}.
     *
     * <p>No-op if {@code exchangeTimestamp} is null — some ticks may have null timestamps
     * due to exchange API differences (e.g., Binance's bookTicker for some pairs).
     *
     * @param exchangeId        the exchange that produced the tick
     * @param exchangeTimestamp the exchange's server timestamp from the tick; may be null
     */
    public void recordTick(ExchangeId exchangeId, Instant exchangeTimestamp) {
        if (exchangeTimestamp == null) {
            log.debug("Null exchangeTimestamp for exchange={} — skipping skew measurement", exchangeId);
            return;
        }

        final long localMs = wallClockMillisSupplier.get();
        final long exchangeMs = exchangeTimestamp.toEpochMilli();
        final long currentOffsetMs = localMs - exchangeMs;

        rollingMeanOffsetMs.compute(exchangeId, (id, existingMean) -> {
            if (existingMean == null) {
                // First observation — seed the EWMA with the current value
                log.debug("Clock skew first observation: exchange={} offsetMs={}", id, currentOffsetMs);
                updateGauge(id, currentOffsetMs);
                return (double) currentOffsetMs;
            }

            // Check for a significant jump relative to the rolling mean
            final long deviationMs = Math.abs(currentOffsetMs - Math.round(existingMean));
            if (deviationMs > jumpThresholdMs) {
                log.warn("Clock skew jump detected: exchange={} currentOffsetMs={} rollingMeanMs={} deviationMs={}",
                        id, currentOffsetMs, Math.round(existingMean), deviationMs);
            } else {
                log.debug("Clock skew: exchange={} offsetMs={} rollingMeanMs={} deviationMs={}",
                        id, currentOffsetMs, Math.round(existingMean), deviationMs);
            }

            // EWMA update: blend new observation with existing mean
            final double newMean = ewmaAlpha * currentOffsetMs + (1.0 - ewmaAlpha) * existingMean;
            updateGauge(id, Math.round(newMean));
            return newMean;
        });
    }

    /**
     * Returns the current rolling mean offset for the given exchange in milliseconds.
     *
     * <p>Returns {@code 0} for exchanges that have not yet produced a tick.
     * The offset is {@code localWallClockMs - exchangeTimestampMs} — positive values
     * indicate the exchange timestamp is in the past (expected), negative values
     * indicate the exchange clock is ahead.
     *
     * @param exchangeId the exchange to query
     * @return rolling mean offset in ms; 0 if exchange not yet seen
     */
    public long getRollingMeanOffsetMs(ExchangeId exchangeId) {
        Double mean = rollingMeanOffsetMs.get(exchangeId);
        return mean == null ? 0L : Math.round(mean);
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /**
     * Updates (or lazily registers) the Micrometer gauge for the given exchange's offset.
     *
     * <p>Gauge metric: {@code normalisation.clock.skew.offset} (tagged by exchange).
     * Unit is milliseconds. Prometheus scrapes the raw long value — configure Grafana
     * to display in ms. Alert threshold: {@code abs(normalisation_clock_skew_offset) > 500}.
     *
     * @param exchangeId the exchange to update
     * @param roundedMeanMs the rounded rolling mean offset in ms
     */
    private void updateGauge(ExchangeId exchangeId, long roundedMeanMs) {
        final AtomicLong gauge = offsetGauges.computeIfAbsent(exchangeId, id -> {
            final AtomicLong value = new AtomicLong(roundedMeanMs);
            meterRegistry.gauge(METRIC_OFFSET,
                    Tags.of(TAG_EXCHANGE, id.name()),
                    value,
                    AtomicLong::doubleValue);
            log.debug("Registered clock skew offset gauge for exchange={}", id.name());
            return value;
        });
        gauge.set(roundedMeanMs);
    }
}
