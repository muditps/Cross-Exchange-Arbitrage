package com.arbitrage.dashboard.analytics;

/**
 * One data point on the cumulative P&amp;L time-series chart.
 *
 * <p>Both profit values are serialised as plain-string BigDecimal to preserve
 * 8dp precision beyond JavaScript's number limit.
 *
 * <p>{@code cumulativeGrossProfit} is the running total of {@code estimated_profit}
 * from {@code arbitrage_opportunities} (theoretical, before slippage).
 * {@code cumulativeSimProfit} is the running total of {@code net_profit} from
 * {@code simulation_results} after slippage modelling; defaults to 0 for
 * opportunities that have no simulation result yet.
 *
 * @param ts                    ISO-8601 UTC timestamp of the CLOSED opportunity
 * @param cumulativeGrossProfit running total theoretical profit as plain-string BigDecimal
 * @param cumulativeSimProfit   running total simulated net profit as plain-string BigDecimal
 */
public record CumulativePnlPointDto(String ts, String cumulativeGrossProfit, String cumulativeSimProfit) {}
