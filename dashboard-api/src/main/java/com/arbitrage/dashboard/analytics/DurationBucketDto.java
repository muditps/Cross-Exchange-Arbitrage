package com.arbitrage.dashboard.analytics;

/**
 * One bucket in the opportunity duration distribution histogram.
 *
 * <p>Duration is {@code total_duration_ms} — from DETECTED to CLOSED/EXPIRED.
 * Buckets: 0–50ms, 50–100ms, 100–200ms, 200–500ms, 500ms–1s, 1s+.
 *
 * @param bucketLabel human-readable duration range label (e.g. "100–200ms")
 * @param count       number of opportunities that fell in this bucket
 * @param bucketOrder 1-based sort position, used by the SQL ORDER BY
 */
public record DurationBucketDto(String bucketLabel, long count, int bucketOrder) {}
