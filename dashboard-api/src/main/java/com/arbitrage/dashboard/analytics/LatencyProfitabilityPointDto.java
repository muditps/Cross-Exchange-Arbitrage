package com.arbitrage.dashboard.analytics;

/**
 * One data point on the Latency vs Profitability scatter chart.
 *
 * <p>This chart answers the fundamental HFT question: "how does execution latency
 * affect whether an opportunity is profitable?" Each point is one closed
 * simulation result — plotted as (execution latency ms, net profit). A linear
 * regression trendline drawn over these points will show the characteristic
 * downward slope: as latency increases, expected profit falls, crossing $0 at the
 * "breakeven latency." Beyond that point, the trade loses money.
 *
 * <p>{@code netProfit} is serialised as a plain-string BigDecimal to preserve
 * 8dp precision. The frontend applies {@code parseFloat()} only at render time
 * for Recharts coordinate mapping — never for financial logic.
 *
 * @param latencyMs  simulated total execution latency in milliseconds
 * @param netProfit  simulated net profit (after fees and slippage) as plain-string BigDecimal
 */
public record LatencyProfitabilityPointDto(long latencyMs, String netProfit) {}
