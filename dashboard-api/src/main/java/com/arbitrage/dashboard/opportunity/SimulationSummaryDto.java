package com.arbitrage.dashboard.opportunity;

/**
 * Execution simulation outcome for one arbitrage opportunity.
 * Read from the {@code simulation_results} hypertable.
 *
 * <p>When an opportunity closes, the execution simulator runs a replay-based
 * simulation to answer: "Could you have captured this?" The result is stored
 * here and surfaced in the opportunity detail modal.
 *
 * @param simulatedLatencyMs  assumed round-trip execution latency (ms)
 * @param slippageBps         price movement during order transit (bps)
 * @param simulatedBuyPrice   price actually paid after slippage (8dp string)
 * @param simulatedSellPrice  price actually received after slippage (8dp string)
 * @param simulatedQuantity   quantity filled (may be less than detected if liquidity dried up)
 * @param wasProfitable       true if net P&L after all costs was positive
 * @param netProfit           actual P&L after fees and slippage (can be negative)
 * @param fillProbability     estimated probability both orders would fill (0.0 – 1.0)
 */
public record SimulationSummaryDto(
        long simulatedLatencyMs,
        String slippageBps,
        String simulatedBuyPrice,
        String simulatedSellPrice,
        String simulatedQuantity,
        boolean wasProfitable,
        String netProfit,
        String fillProbability
) {}
