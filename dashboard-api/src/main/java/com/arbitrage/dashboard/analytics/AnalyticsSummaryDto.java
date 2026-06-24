package com.arbitrage.dashboard.analytics;

/**
 * Aggregate analytics summary for a given time window.
 *
 * <p>Combines data from two tables:
 * <ul>
 *   <li>{@code arbitrage_opportunities} — opportunity counts, spread statistics</li>
 *   <li>{@code simulation_results} — execution simulation win rate</li>
 * </ul>
 *
 * <p>All numeric fields default to 0 when no data exists in the window.
 *
 * @param totalOpportunities    total opportunities detected in the window
 * @param closedOpportunities   opportunities that reached CLOSED status
 * @param expiredOpportunities  opportunities that reached EXPIRED status (timed out)
 * @param activeOpportunities   opportunities currently in DETECTED or OPEN state
 * @param avgNetSpreadBps       average net spread across all opportunities (bps)
 * @param maxNetSpreadBps       highest net spread observed in the window (bps)
 * @param totalSimulations      execution simulations run in the window
 * @param profitableSimulations simulations where was_profitable = true
 * @param winRatePct            profitable / total * 100 (0 when no simulations)
 * @param windowHours           the hours parameter used to generate this summary
 */
public record AnalyticsSummaryDto(
        long totalOpportunities,
        long closedOpportunities,
        long expiredOpportunities,
        long activeOpportunities,
        double avgNetSpreadBps,
        double maxNetSpreadBps,
        long totalSimulations,
        long profitableSimulations,
        double winRatePct,
        int windowHours
) {}
