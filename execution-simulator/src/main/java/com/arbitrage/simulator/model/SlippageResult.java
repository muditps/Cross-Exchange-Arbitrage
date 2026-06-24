package com.arbitrage.simulator.model;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * Immutable result of a slippage estimation for a two-legged arbitrage trade.
 *
 * <p><b>What slippage is:</b> The difference between the price we expected to pay/receive
 * at detection time and the price we actually paid/received at fill time. Markets move
 * during the execution window (27–52ms). A rising buy-side ask or a falling sell-side bid
 * reduces profitability vs the detection-time snapshot.
 *
 * <p><b>Sign convention — positive = adverse:</b>
 * <ul>
 *   <li>{@link #buySlippageBps} is positive when the execution ask is HIGHER than the
 *       detection ask — we paid more than expected.</li>
 *   <li>{@link #sellSlippageBps} is positive when the execution bid is LOWER than the
 *       detection bid — we received less than expected.</li>
 *   <li>A negative value on either leg means a favorable price move during execution
 *       (rare in fast-moving crypto markets).</li>
 * </ul>
 *
 * <p><b>Fallback mode ({@code priceDataAvailable = false}):</b>
 * When {@link com.arbitrage.simulator.service.PriceAtExecutionLookup} returns empty for
 * either leg (price store not yet warm after startup), the estimator falls back to
 * detection-time prices and applies the configured {@code defaultSlippageBps} per leg.
 * Simulation results produced in fallback mode are less accurate but still valid —
 * they use a worst-case assumption rather than observed market data.
 *
 * <p><b>Scale:</b> All {@code BigDecimal} fields are stored at scale 8 with HALF_UP
 * rounding for prices, and scale 4 for bps values (1 bps = 0.01%, 4 decimal places
 * gives 0.0001 bps precision — sufficient for simulation purposes).
 */
@Value
@Builder
public class SlippageResult {

    /**
     * The actual ask price on the buy exchange at simulated fill time.
     *
     * <p>Retrieved from the {@link com.arbitrage.simulator.service.HistoricalPriceStore}
     * via floor lookup at {@code detectionTimestamp + executionLatencyMs}. Falls back to
     * the detection-time ask price when the store is empty.
     */
    BigDecimal buyExecutionPrice;

    /**
     * The actual bid price on the sell exchange at simulated fill time.
     *
     * <p>Retrieved from the {@link com.arbitrage.simulator.service.HistoricalPriceStore}
     * via floor lookup at {@code detectionTimestamp + executionLatencyMs}. Falls back to
     * the detection-time bid price when the store is empty.
     */
    BigDecimal sellExecutionPrice;

    /**
     * Slippage on the buy leg in basis points.
     *
     * <p>Formula: {@code (buyExecutionAsk − detectionBuyAsk) / detectionBuyAsk × 10,000}.
     * Positive = adverse (paid more than detection price). Negative = favorable (paid less).
     */
    BigDecimal buySlippageBps;

    /**
     * Slippage on the sell leg in basis points.
     *
     * <p>Formula: {@code (detectionSellBid − sellExecutionBid) / detectionSellBid × 10,000}.
     * Positive = adverse (received less than detection price). Negative = favorable.
     */
    BigDecimal sellSlippageBps;

    /**
     * Combined slippage across both legs: {@code buySlippageBps + sellSlippageBps}.
     *
     * <p>This is the total cost of market movement during the execution window.
     * A positive total means the opportunity was less profitable at fill time than at
     * detection time. If {@code totalSlippageBps ≥ netSpreadBps}, the trade was
     * unprofitable despite a positive detection-time net spread.
     */
    BigDecimal totalSlippageBps;

    /**
     * Whether live price data was available from the {@link com.arbitrage.simulator.service.HistoricalPriceStore}
     * for both legs at the simulated fill time.
     *
     * <p>{@code false} indicates the estimator fell back to detection-time prices plus the
     * configured {@code defaultSlippageBps} — this happens on startup before the price
     * store has accumulated enough history. Results with {@code priceDataAvailable = false}
     * should be weighted less heavily in analytics.
     */
    boolean priceDataAvailable;
}
