package com.arbitrage.dashboard.analytics;

/**
 * Opportunity count aggregated to a one-hour bucket.
 *
 * <p>Used to populate the hourly bar chart on the Analytics page.
 * The {@code hour} field is an ISO-8601 string produced by
 * {@code date_trunc('hour', detection_timestamp)} — e.g. {@code "2026-06-07T14:00:00Z"}.
 *
 * @param hour  ISO-8601 timestamp of the start of the hour bucket
 * @param count number of opportunities detected in that hour
 */
public record HourlyCountDto(String hour, long count) {}
