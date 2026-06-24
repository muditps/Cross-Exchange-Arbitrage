package com.arbitrage.dashboard.admin;

/**
 * Response payload for data cleanup operations.
 *
 * @param scope                  "ALL" or the trading pair that was deleted (e.g., "BTC-USDT")
 * @param opportunitiesDeleted   rows deleted from {@code arbitrage_opportunities}
 * @param simulationsDeleted     rows deleted from {@code simulation_results}
 */
public record DeleteResult(
        String scope,
        int opportunitiesDeleted,
        int simulationsDeleted
) {}
