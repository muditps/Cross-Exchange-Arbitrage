package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Monitors the health of normalised-tick feeds from each exchange.
 *
 * <p>In trading systems, a <em>stale feed is worse than a dead feed.</em> A dead feed
 * produces an obvious error. A stale feed silently serves old prices that look valid but
 * reflect the market state from seconds ago — a dangerous input to any detection algorithm.
 *
 * <p><b>State machine per exchange:</b>
 * <pre>
 *   (initial / never seen) ──────────────────────────────▶ DISCONNECTED
 *
 *   DISCONNECTED  ──── tick received ──────────────────▶  CONNECTED
 *   CONNECTED     ──── tick received ──────────────────▶  CONNECTED  (refresh)
 *   CONNECTED     ──── age &gt; staleThreshold ───────────▶  STALE
 *   STALE         ──── tick received ──────────────────▶  CONNECTED  (recovery)
 *   STALE         ──── age &gt; disconnectedThreshold ────▶  DISCONNECTED
 *   DISCONNECTED  ──── age &gt; disconnectedThreshold ────▶  DISCONNECTED  (stays)
 * </pre>
 *
 * <p><b>Why two thresholds?</b> STALE and DISCONNECTED are operationally different alerts.
 * STALE (5s default): "Something is wrong — investigate." Maybe the exchange is slow,
 * maybe one partition is paused. DISCONNECTED (30s default): "The feed has definitely
 * dropped — page the on-call." Different thresholds allow tiered alerting with different
 * severities in Grafana/PagerDuty.
 *
 * <p><b>Thread safety:</b> {@link ConcurrentHashMap} is used for both maps. The
 * {@link #recordTickReceived} method is called from up to 3 Kafka consumer threads
 * (one per exchange topic) concurrently. The {@link #checkFeedHealth()} scheduler
 * runs on a Spring scheduler thread. ConcurrentHashMap's visibility guarantees ensure
 * that the scheduler always reads the latest {@code lastSeenNanos} values.
 *
 * <p><b>Time injection for testability:</b> The clock is injected as a
 * {@code Supplier<Long>} returning nanoseconds. Production uses {@code System::nanoTime}.
 * Tests pass a controlled {@code AtomicLong::get} — no {@code Thread.sleep} required.
 * This pattern is essential for testing time-based behavior deterministically.
 *
 * <p><b>Metrics:</b> A Micrometer gauge {@code normalisation.feed.status} is registered
 * per exchange, backed by an {@link AtomicInteger} with the {@link FeedStatus#ordinal()} value.
 * In Grafana: configure value mappings so 0=CONNECTED (green), 1=RECONNECTING (yellow),
 * 2=STALE (orange), 3=DISCONNECTED (red).
 *
 * @see FeedStatus the status enum with Javadoc explaining each state
 * @see NormalisationService calls {@link #recordTickReceived} after a successful transformation
 */
@Component
@Slf4j
public class FeedHealthMonitor {

    private static final String METRIC_FEED_STATUS = "normalisation.feed.status";
    private static final String TAG_EXCHANGE = "exchange";

    /**
     * Timestamp (nanoseconds from {@code System.nanoTime()} epoch) of the most
     * recently received valid normalised tick per exchange.
     *
     * <p>Null entry means the exchange has never produced a valid tick since startup.
     * ConcurrentHashMap because Kafka consumer threads and the scheduler thread access
     * this map concurrently.
     */
    private final Map<ExchangeId, Long> lastSeenNanos = new ConcurrentHashMap<>();

    /**
     * Current health status per exchange. Default (absent key) = DISCONNECTED —
     * before any tick arrives, the normalisation layer has not seen data from the exchange.
     */
    private final Map<ExchangeId, FeedStatus> feedStatuses = new ConcurrentHashMap<>();

    /**
     * Gauge backing values for Micrometer. Stores the ordinal of the current
     * {@link FeedStatus} per exchange. Lazily registered on first status transition.
     */
    private final Map<ExchangeId, AtomicInteger> statusGauges = new ConcurrentHashMap<>();

    /**
     * Supplier for the current time in nanoseconds. Uses {@code System::nanoTime}
     * in production; injected in tests for deterministic time control.
     */
    private final Supplier<Long> currentNanosSupplier;

    /**
     * Duration after which a feed transitions from CONNECTED to STALE, in nanoseconds.
     */
    private final long staleThresholdNanos;

    /**
     * Duration after which a feed transitions from STALE to DISCONNECTED, in nanoseconds.
     */
    private final long disconnectedThresholdNanos;

    private final MeterRegistry meterRegistry;

    /**
     * Production constructor. Spring injects thresholds from application properties.
     *
     * @param staleThresholdMs        milliseconds before a feed is considered stale (default 5000ms)
     * @param disconnectedThresholdMs milliseconds before a feed is considered disconnected (default 30000ms)
     * @param meterRegistry           Micrometer registry for feed status gauges
     */
    @Autowired
    public FeedHealthMonitor(
            @Value("${arbitrage.normalisation.feed-health.stale-threshold-ms:5000}") long staleThresholdMs,
            @Value("${arbitrage.normalisation.feed-health.disconnected-threshold-ms:30000}") long disconnectedThresholdMs,
            MeterRegistry meterRegistry) {
        this(System::nanoTime, staleThresholdMs, disconnectedThresholdMs, meterRegistry);
    }

    /**
     * Test constructor. Allows injecting a controlled clock for deterministic time-based tests.
     *
     * <p><b>Why package-private?</b> This constructor exists only to enable testing without
     * {@code Thread.sleep}. It is not part of the public API. Package-private access prevents
     * accidental use outside the test package while still allowing test classes in the same
     * package to construct the monitor with a fake clock.
     *
     * @param currentNanosSupplier    clock source — use {@code AtomicLong::get} in tests
     * @param staleThresholdMs        milliseconds before a feed is considered stale
     * @param disconnectedThresholdMs milliseconds before a feed is considered disconnected
     * @param meterRegistry           Micrometer registry for feed status gauges
     */
    FeedHealthMonitor(
            Supplier<Long> currentNanosSupplier,
            long staleThresholdMs,
            long disconnectedThresholdMs,
            MeterRegistry meterRegistry) {
        this.currentNanosSupplier = currentNanosSupplier;
        this.staleThresholdNanos = staleThresholdMs * 1_000_000L;
        this.disconnectedThresholdNanos = disconnectedThresholdMs * 1_000_000L;
        this.meterRegistry = meterRegistry;
        log.info("FeedHealthMonitor initialised: staleThresholdMs={} disconnectedThresholdMs={}",
                staleThresholdMs, disconnectedThresholdMs);
    }

    // ========================================================================
    // Public API — called by NormalisationService
    // ========================================================================

    /**
     * Records that a valid normalised tick was received from the given exchange.
     *
     * <p>Updates the last-seen timestamp and transitions the feed status to
     * {@link FeedStatus#CONNECTED}. This is a recovery path for STALE and DISCONNECTED feeds.
     *
     * <p>Called from Kafka consumer threads — must be fast and non-blocking.
     * ConcurrentHashMap.put is O(1) and lock-free under normal load.
     *
     * @param exchangeId the exchange that produced the tick
     */
    public void recordTickReceived(ExchangeId exchangeId) {
        lastSeenNanos.put(exchangeId, currentNanosSupplier.get());
        transitionTo(exchangeId, FeedStatus.CONNECTED);
    }

    /**
     * Returns the current health status of the given exchange's feed.
     *
     * <p>Returns {@link FeedStatus#DISCONNECTED} for exchanges that have never produced
     * a valid tick (unseen since startup). This is conservative — no data is treated as
     * disconnected until proven otherwise.
     *
     * @param exchangeId the exchange to query
     * @return current feed status; never null
     */
    public FeedStatus getStatus(ExchangeId exchangeId) {
        return feedStatuses.getOrDefault(exchangeId, FeedStatus.DISCONNECTED);
    }

    /**
     * Returns the age in milliseconds of the most recently received tick from the given exchange.
     *
     * <p>Uses the same clock supplier as {@link #checkFeedHealth()} for consistency.
     * Returns an empty {@link java.util.OptionalLong} if no tick has ever been received from
     * this exchange since startup — callers should treat this as "never seen."
     *
     * @param exchangeId the exchange to query
     * @return age of last tick in milliseconds, or empty if the exchange has never produced a tick
     */
    public java.util.OptionalLong getLastSeenAgeMs(ExchangeId exchangeId) {
        final Long lastSeen = lastSeenNanos.get(exchangeId);
        if (lastSeen == null) {
            return java.util.OptionalLong.empty();
        }
        final long ageNanos = currentNanosSupplier.get() - lastSeen;
        return java.util.OptionalLong.of(Math.max(0L, ageNanos / 1_000_000L));
    }

    // ========================================================================
    // Scheduled health check
    // ========================================================================

    /**
     * Periodically checks all known exchanges for stale or disconnected feeds.
     *
     * <p>Runs every {@code check-interval-ms} milliseconds (default 1000ms = 1 second).
     * For each exchange that has been seen at least once, computes the age of the last
     * tick and applies the transition rules:
     * <ul>
     *   <li>age &gt; {@code disconnectedThresholdNanos} → {@link FeedStatus#DISCONNECTED}</li>
     *   <li>age &gt; {@code staleThresholdNanos} and currently CONNECTED → {@link FeedStatus#STALE}</li>
     * </ul>
     *
     * <p>Exchanges that have never produced a tick (null lastSeen) are skipped — they
     * are implicitly DISCONNECTED and do not need a scheduler-driven transition.
     *
     * <p><b>Why check all values in ExchangeId?</b> The monitor tracks exchanges by their
     * enum ordinals. Iterating the enum values is O(n) with n=3. At 1s intervals, this is
     * negligible overhead.
     *
     * <p><b>Why {@code fixedDelay} not {@code fixedRate}?</b> {@code fixedDelay} starts the
     * next invocation after the current one finishes. {@code fixedRate} starts on a schedule
     * regardless. For a health check, we want to avoid overlapping invocations if the
     * scheduler is delayed — {@code fixedDelay} is safer.
     */
    @Scheduled(fixedDelayString = "${arbitrage.normalisation.feed-health.check-interval-ms:1000}")
    public void checkFeedHealth() {
        final long now = currentNanosSupplier.get();

        for (ExchangeId exchangeId : ExchangeId.values()) {
            Long lastSeen = lastSeenNanos.get(exchangeId);
            if (lastSeen == null) {
                // Never seen — implicitly DISCONNECTED, no transition needed
                continue;
            }

            final long ageNanos = now - lastSeen;
            final FeedStatus currentStatus = feedStatuses.getOrDefault(exchangeId, FeedStatus.CONNECTED);

            if (ageNanos > disconnectedThresholdNanos) {
                transitionTo(exchangeId, FeedStatus.DISCONNECTED);
            } else if (ageNanos > staleThresholdNanos && currentStatus == FeedStatus.CONNECTED) {
                transitionTo(exchangeId, FeedStatus.STALE);
            }
            // STALE → CONNECTED is handled only in recordTickReceived, not here.
            // STALE → DISCONNECTED is handled above (age > disconnected threshold).
        }
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /**
     * Transitions an exchange to a new status, logging the change and updating the Micrometer gauge.
     *
     * <p>Only performs a gauge update and log when the status actually changes — prevents
     * log spam on every scheduler tick for a feed that has been DISCONNECTED for minutes.
     *
     * @param exchangeId the exchange whose status is changing
     * @param newStatus  the target status
     */
    private void transitionTo(ExchangeId exchangeId, FeedStatus newStatus) {
        final FeedStatus previous = feedStatuses.put(exchangeId, newStatus);
        if (previous == newStatus) {
            return; // No change — avoid redundant logging and gauge updates
        }

        if (previous == null || newStatus == FeedStatus.CONNECTED) {
            log.info("Feed status: exchange={} status={}", exchangeId, newStatus);
        } else if (newStatus == FeedStatus.STALE) {
            log.warn("Feed STALE: exchange={} — no valid tick received within threshold", exchangeId);
        } else if (newStatus == FeedStatus.DISCONNECTED) {
            log.error("Feed DISCONNECTED: exchange={} — no valid tick received within disconnected threshold", exchangeId);
        }

        updateStatusGauge(exchangeId, newStatus);
    }

    /**
     * Updates (or lazily registers) the Micrometer gauge for the given exchange's feed status.
     *
     * <p>Gauges in Micrometer require a numeric supplier. We back each gauge with an
     * {@link AtomicInteger} holding the {@link FeedStatus#ordinal()} value. Prometheus
     * scrapes the current integer value. In Grafana, configure value mappings:
     * 0=CONNECTED, 1=RECONNECTING, 2=STALE, 3=DISCONNECTED.
     *
     * @param exchangeId the exchange to update
     * @param status     the new status
     */
    private void updateStatusGauge(ExchangeId exchangeId, FeedStatus status) {
        final AtomicInteger gauge = statusGauges.computeIfAbsent(exchangeId, id -> {
            final AtomicInteger value = new AtomicInteger(status.ordinal());
            meterRegistry.gauge(METRIC_FEED_STATUS,
                    Tags.of(TAG_EXCHANGE, id.name()),
                    value);
            log.debug("Registered feed status gauge for exchange={}", id.name());
            return value;
        });
        gauge.set(status.ordinal());
    }
}
