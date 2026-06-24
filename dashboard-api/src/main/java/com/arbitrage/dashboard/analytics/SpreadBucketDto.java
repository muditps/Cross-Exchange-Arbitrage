package com.arbitrage.dashboard.analytics;

/**
 * One bucket in the net-spread distribution histogram.
 *
 * <p>Buckets are defined by the repository query as fixed bps ranges:
 * Negative, 0–5, 5–10, 10–20, 20–50, 50+. {@code bucketOrder} is the
 * sort key; the list is always returned ordered ascending by it.
 *
 * @param bucketLabel human-readable range label (e.g. "0–5 bps")
 * @param count       number of opportunities that fell in this bucket
 * @param bucketOrder 1-based sort position, used by the SQL ORDER BY
 */
public record SpreadBucketDto(String bucketLabel, long count, int bucketOrder) {}
