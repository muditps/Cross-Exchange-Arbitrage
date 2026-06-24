package com.arbitrage.dashboard.analytics;

/**
 * One hourly data point on the False Positive Rate over time chart.
 *
 * <p>A "false positive" is an opportunity that was detected but was not
 * actually executable at a profit — either it expired before any execution
 * could have completed, or the execution simulator determined it would have
 * lost money. A high false positive rate suggests the detection engine is
 * generating noise: spreading that look large during a single tick update
 * but are not persistent enough to trade.
 *
 * <p>The rate is pre-computed as {@code falsePositiveCount / totalCount * 100.0}
 * in the repository layer so the frontend can plot it directly without
 * division-by-zero risk.
 *
 * <p>Only CLOSED and EXPIRED opportunities are included in the denominator
 * (DETECTED/OPEN have not resolved yet and cannot be classified).
 *
 * @param hour              ISO-8601 UTC timestamp for the start of this hourly bucket
 * @param totalCount        total CLOSED + EXPIRED opportunities in this hour
 * @param falsePositiveCount opportunities in this hour that were EXPIRED or simulated as unprofitable
 * @param falsePositiveRate  {@code falsePositiveCount / totalCount × 100}, range 0–100
 */
public record FalsePositiveRatePointDto(
        String hour,
        long totalCount,
        long falsePositiveCount,
        double falsePositiveRate) {}
