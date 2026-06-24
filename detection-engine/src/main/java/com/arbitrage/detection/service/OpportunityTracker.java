package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.model.OpportunityKey;
import com.arbitrage.common.model.TradingPair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Tracks arbitrage opportunities through their lifecycle: DETECTED → OPEN → CLOSED/EXPIRED.
 *
 * <p><b>Why this exists:</b> Without lifecycle tracking, every tick with a positive
 * net spread generates a separate DETECTED event. At 100ms tick rate across 3 exchanges,
 * one persistent spread produces ~30 events/second — thousands of duplicates per minute.
 * The tracker collapses these into one DETECTED event, silent updates, and one terminal
 * event (CLOSED or EXPIRED).
 *
 * <p><b>State machine:</b>
 * <pre>
 *   First tick with netSpread &gt; 0:       → store as OPEN, emit DETECTED event
 *   Subsequent ticks, still profitable:   → update peak/average/count in map, no event
 *   First tick with netSpread ≤ 0:        → remove from map, emit CLOSED event
 *   OPEN longer than maxOpportunityDurationMs: → remove from map, emit EXPIRED event
 * </pre>
 *
 * <p><b>Pair scoping:</b> Closure detection only considers OPEN entries whose key matches
 * the pair being processed. Opportunities for other pairs are unaffected by the current tick.
 *
 * <p><b>Threading:</b> Called from Kafka's consumer thread (single thread per partition).
 * The background expiry sweep ({@link #runExpiry}) runs on Spring's scheduler thread.
 * Both paths operate on {@link ConcurrentHashMap} whose individual operations are atomic.
 * The only observable interleaving is an entry being expired between a closure check and
 * removal — both paths end with the entry absent, which is correct.
 *
 * <p><b>Running average:</b> {@code averageNetSpread} is maintained using the Welford
 * online algorithm: {@code mean_n = mean_{n-1} + (x_n - mean_{n-1}) / n}.
 * This avoids summing all samples — only the previous mean and current count are needed.
 */
@Service
@Slf4j
public class OpportunityTracker {

    private static final long NS_PER_MS = 1_000_000L;
    private static final int AVERAGE_SCALE = 8;

    private final DetectionProperties detectionProperties;
    private final Supplier<Long> nanoTimeSource;
    private final ConcurrentHashMap<OpportunityKey, ArbitrageOpportunity> openOpportunities =
            new ConcurrentHashMap<>();

    /**
     * Production constructor — Spring-managed. Uses the system monotonic clock.
     */
    @Autowired
    public OpportunityTracker(DetectionProperties detectionProperties) {
        this(detectionProperties, System::nanoTime);
    }

    /**
     * Test constructor — injectable clock for deterministic expiry tests.
     *
     * <p>Pass an {@code AtomicLong::get} lambda to control perceived time.
     * The production constructor delegates here with {@code System::nanoTime}.
     */
    OpportunityTracker(DetectionProperties detectionProperties, Supplier<Long> nanoTimeSource) {
        this.detectionProperties = detectionProperties;
        this.nanoTimeSource      = nanoTimeSource;
    }

    /**
     * Applies the lifecycle state machine to a fresh batch of profitable detections for one pair.
     *
     * <p>Three outcomes per direction in the current tick:
     * <ol>
     *   <li><b>New direction (key not in map):</b> stored as OPEN; DETECTED event returned.</li>
     *   <li><b>Known direction (key in map, still profitable):</b> peak/average/count updated
     *       in map; no event returned (not a new or terminal state).</li>
     *   <li><b>Known direction (key in map, absent from fresh set):</b> transitioned to CLOSED,
     *       final duration computed, removed from map; CLOSED event returned.</li>
     * </ol>
     *
     * @param pair            trading pair from the current tick — used to scope closure detection
     * @param freshDetections profitable directions found this tick; each has {@code status=DETECTED}
     * @return publishable events: DETECTED for new opportunities, CLOSED for just-closed ones
     */
    public List<ArbitrageOpportunity> update(TradingPair pair, List<ArbitrageOpportunity> freshDetections) {
        List<ArbitrageOpportunity> events = new ArrayList<>();
        long nanoNow = nanoTimeSource.get();

        // Build a fast-lookup set of keys in the current profitable batch
        Set<OpportunityKey> freshKeys = new HashSet<>();
        for (ArbitrageOpportunity opp : freshDetections) {
            freshKeys.add(toKey(opp));
        }

        // Step 1: process each fresh profitable direction
        for (ArbitrageOpportunity fresh : freshDetections) {
            OpportunityKey key = toKey(fresh);
            ArbitrageOpportunity existing = openOpportunities.get(key);

            if (existing == null) {
                // New direction: store as OPEN, emit DETECTED event
                ArbitrageOpportunity opened = fresh.toBuilder()
                        .status(OpportunityStatus.OPEN)
                        .updateCount(1L)
                        .build();
                openOpportunities.put(key, opened);
                events.add(fresh); // fresh has status=DETECTED — that's the publishable event
                log.info("Opportunity opened: pair={} sellExchange={} buyExchange={} netSpread={}",
                        pair.canonicalSymbol(), fresh.getSellExchange(), fresh.getBuyExchange(),
                        fresh.getNetSpread());
            } else {
                // Known direction: update tracking metrics — no event
                long newCount = existing.getUpdateCount() + 1;
                BigDecimal newNetSpread = fresh.getNetSpread();

                BigDecimal newPeak = existing.getPeakNetSpread().compareTo(newNetSpread) >= 0
                        ? existing.getPeakNetSpread()
                        : newNetSpread;

                // Welford online algorithm: mean_n = mean_{n-1} + (x_n - mean_{n-1}) / n
                BigDecimal delta = newNetSpread.subtract(existing.getAverageNetSpread());
                BigDecimal newAverage = existing.getAverageNetSpread()
                        .add(delta.divide(BigDecimal.valueOf(newCount), AVERAGE_SCALE, RoundingMode.HALF_UP));

                ArbitrageOpportunity updated = existing.toBuilder()
                        .netSpread(newNetSpread)
                        .grossSpread(fresh.getGrossSpread())
                        .grossSpreadBps(fresh.getGrossSpreadBps())
                        .netSpreadBps(fresh.getNetSpreadBps())
                        .theoreticalProfit(fresh.getTheoreticalProfit())
                        .peakNetSpread(newPeak)
                        .averageNetSpread(newAverage)
                        .updateCount(newCount)
                        .lastUpdateTimestamp(Instant.now())
                        .build();
                openOpportunities.put(key, updated);
                log.debug("Opportunity updated: pair={} sellExchange={} buyExchange={} netSpread={} peak={} count={}",
                        pair.canonicalSymbol(), fresh.getSellExchange(), fresh.getBuyExchange(),
                        newNetSpread, newPeak, newCount);
            }
        }

        // Step 2: close OPEN entries for this pair that are no longer profitable
        for (Map.Entry<OpportunityKey, ArbitrageOpportunity> entry : openOpportunities.entrySet()) {
            OpportunityKey key = entry.getKey();
            if (!key.getTradingPair().equals(pair)) continue;
            if (freshKeys.contains(key)) continue;

            ArbitrageOpportunity open = entry.getValue();
            long durationMs = (nanoNow - open.getDetectedNanoTime()) / NS_PER_MS;
            ArbitrageOpportunity closed = open.toBuilder()
                    .status(OpportunityStatus.CLOSED)
                    .closedNanoTime(nanoNow)
                    .totalDurationMs(durationMs)
                    .lastUpdateTimestamp(Instant.now())
                    .build();
            openOpportunities.remove(key);
            events.add(closed);
            log.info("Opportunity closed: pair={} sellExchange={} buyExchange={} durationMs={} peakNetSpread={}",
                    pair.canonicalSymbol(), open.getSellExchange(), open.getBuyExchange(),
                    durationMs, open.getPeakNetSpread());
        }

        return events;
    }

    /**
     * Scans all OPEN entries and expires any that have been open longer than
     * {@link DetectionProperties#getMaxOpportunityDurationMs()}.
     *
     * <p>Package-private for direct testing without relying on the scheduler.
     *
     * @return list of EXPIRED opportunities removed from the tracker
     */
    List<ArbitrageOpportunity> expireStaleOpportunities() {
        long nanoNow = nanoTimeSource.get();
        long maxDurationNs = detectionProperties.getMaxOpportunityDurationMs() * NS_PER_MS;
        List<ArbitrageOpportunity> expired = new ArrayList<>();

        openOpportunities.entrySet().removeIf(entry -> {
            ArbitrageOpportunity opp = entry.getValue();
            long ageNs = nanoNow - opp.getDetectedNanoTime();
            if (ageNs > maxDurationNs) {
                long durationMs = ageNs / NS_PER_MS;
                ArbitrageOpportunity expiredOpp = opp.toBuilder()
                        .status(OpportunityStatus.EXPIRED)
                        .closedNanoTime(nanoNow)
                        .totalDurationMs(durationMs)
                        .lastUpdateTimestamp(Instant.now())
                        .build();
                expired.add(expiredOpp);
                log.warn("Opportunity expired: pair={} sellExchange={} buyExchange={} durationMs={} — likely data artefact",
                        opp.getTradingPair().canonicalSymbol(), opp.getSellExchange(),
                        opp.getBuyExchange(), durationMs);
                return true;
            }
            return false;
        });

        return expired;
    }

    /**
     * Returns the current number of tracked OPEN opportunities.
     * Package-private — used in tests and future metrics.
     */
    int openCount() {
        return openOpportunities.size();
    }

    private OpportunityKey toKey(ArbitrageOpportunity opp) {
        return new OpportunityKey(opp.getTradingPair(), opp.getSellExchange(), opp.getBuyExchange());
    }
}
