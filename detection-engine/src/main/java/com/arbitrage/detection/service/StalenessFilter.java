package com.arbitrage.detection.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.model.PriceState;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Determines whether a cached price is too old to use in arbitrage comparisons.
 *
 * <p><b>Why staleness filtering matters:</b> Redis can hold a price for up to
 * {@link DetectionProperties#getRedisPriceTtlMs()} (10s). During that window, the price
 * might be 200ms, 1s, or 5s old. Comparing a 200ms-old Binance price against a fresh
 * KuCoin price can produce a "spread" that disappeared 180ms ago — a phantom arbitrage
 * signal. Staleness filtering rejects prices older than the configured threshold
 * ({@link DetectionProperties#getStalenessThresholdMs()}, default 500ms) at the moment
 * of comparison.
 *
 * <p><b>Why {@code receivedTimestamp} and NOT {@code exchangeTimestamp}?</b>
 * {@code exchangeTimestamp} is set by the exchange's server clock, which may be skewed
 * by hundreds of milliseconds relative to our local clock. Binance's server and KuCoin's
 * server do not share a clock — their timestamps are not directly comparable. Using
 * {@code receivedTimestamp} ({@code System.nanoTime()} stamped when the WebSocket message
 * arrived at our connector) anchors all age calculations to our own monotonic clock.
 * This is the standard approach in distributed feed handlers.
 *
 * <p><b>Why {@code >} (strict) not {@code >=} (inclusive)?</b>
 * A price at exactly the threshold age is still usable — it is 500ms old, not stale yet.
 * Staleness begins the instant the price exceeds the threshold: {@code age > threshold}.
 *
 * <p><b>Time injection for testability:</b> The clock is injected as a
 * {@code Supplier<Long>} returning nanoseconds. Production uses {@code System::nanoTime}.
 * Tests inject a fixed or controlled value so timing assertions are deterministic.
 *
 * <p><b>Metrics:</b> Every stale skip increments {@code detection.stale.skips}
 * with an {@code exchange} tag for per-exchange visibility in Grafana. This helps
 * identify which exchange's feed has the most staleness issues during production monitoring.
 */
@Component
@Slf4j
public class StalenessFilter {

    static final String METRIC_STALE_SKIPS = "detection.stale.skips";
    static final String TAG_EXCHANGE       = "exchange";

    private static final long NS_PER_MS = 1_000_000L;

    private final DetectionProperties detectionProperties;
    private final Supplier<Long> nanoTimeSource;
    private final MeterRegistry meterRegistry;

    /**
     * Production constructor — Spring-managed. Uses the system monotonic clock.
     *
     * @param detectionProperties configuration for staleness threshold
     * @param meterRegistry       Micrometer registry for stale-skip metrics
     */
    @Autowired
    public StalenessFilter(DetectionProperties detectionProperties, MeterRegistry meterRegistry) {
        this(detectionProperties, System::nanoTime, meterRegistry);
    }

    /**
     * Test constructor — injectable clock for deterministic time-based tests.
     *
     * <p>Pass an {@code AtomicLong::get} or a fixed {@code () -> fixedNanos} lambda
     * to control perceived time. The production constructor calls this with
     * {@code System::nanoTime}.
     *
     * @param detectionProperties configuration for staleness threshold
     * @param nanoTimeSource       clock source returning nanoseconds
     * @param meterRegistry        Micrometer registry for stale-skip metrics
     */
    StalenessFilter(DetectionProperties detectionProperties,
                    Supplier<Long> nanoTimeSource,
                    MeterRegistry meterRegistry) {
        this.detectionProperties = detectionProperties;
        this.nanoTimeSource      = nanoTimeSource;
        this.meterRegistry       = meterRegistry;
    }

    /**
     * Returns {@code true} if the price is older than the configured staleness threshold.
     *
     * <p>Staleness is computed against {@link PriceState#getReceivedTimestamp()} —
     * the {@code System.nanoTime()} value captured when the WebSocket message arrived
     * at the connector. This is our own monotonic clock, unaffected by exchange clock skew.
     *
     * @param priceState the cached price to evaluate
     * @return {@code true} if {@code currentNanos - receivedTimestamp > thresholdNs}
     */
    public boolean isStale(PriceState priceState) {
        long ageNs        = nanoTimeSource.get() - priceState.getReceivedTimestamp();
        long thresholdNs  = detectionProperties.getStalenessThresholdMs() * NS_PER_MS;
        return ageNs > thresholdNs;
    }

    /**
     * Logs a WARN-level message and increments the stale-skip metric when a direction
     * is skipped due to a stale price.
     *
     * <p>Called by {@link ArbitrageDetectionEngine#compareAllDirections} immediately after
     * {@link #isStale} returns {@code true}. The age in milliseconds is recomputed
     * at call time for accurate logging.
     *
     * @param exchangeId the exchange whose price was stale
     * @param pair       the trading pair being compared
     * @param priceState the stale price state (used to compute age for the log message)
     */
    public void recordStaleSkip(ExchangeId exchangeId, TradingPair pair, PriceState priceState) {
        long ageMs = (nanoTimeSource.get() - priceState.getReceivedTimestamp()) / NS_PER_MS;
        log.warn("Stale price skipped: exchange={} pair={} ageMs={} thresholdMs={}",
                exchangeId, pair.canonicalSymbol(), ageMs,
                detectionProperties.getStalenessThresholdMs());
        meterRegistry.counter(METRIC_STALE_SKIPS,
                TAG_EXCHANGE, exchangeId.name().toLowerCase()).increment();
    }
}
