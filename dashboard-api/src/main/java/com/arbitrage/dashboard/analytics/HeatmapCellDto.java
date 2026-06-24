package com.arbitrage.dashboard.analytics;

/**
 * One cell in the 7-day × 24-hour profitability heatmap.
 *
 * <p>The heatmap answers: "which hours and days are most productive for
 * arbitrage?" It groups all detected opportunities by the UTC day-of-week
 * and hour-of-day at which they were first detected, then computes the
 * average net spread (in basis points) for that bucket. A darker cell = more
 * profitable on average. Empty cells (no opportunities in that bucket) are
 * absent from the list and rendered as grey on the frontend.
 *
 * <p>PostgreSQL {@code EXTRACT(DOW FROM ...)} returns 0=Sunday through 6=Saturday.
 * The frontend maps these integers to day labels.
 *
 * @param dayOfWeek        0 (Sunday) through 6 (Saturday)
 * @param hourOfDay        0 through 23 (UTC hour)
 * @param avgNetSpreadBps  average net spread in basis points for this day/hour bucket
 * @param count            number of opportunities detected in this bucket
 */
public record HeatmapCellDto(int dayOfWeek, int hourOfDay, double avgNetSpreadBps, long count) {}
