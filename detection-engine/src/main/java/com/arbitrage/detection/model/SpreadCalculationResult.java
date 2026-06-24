package com.arbitrage.detection.model;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * Immutable result of a single-direction spread calculation between two exchanges.
 *
 * <p>Produced by {@link com.arbitrage.detection.service.SpreadCalculator} for one
 * ordered (sell, buy) exchange pair. The caller ({@code ArbitrageDetectionEngine})
 * inspects this result to decide whether to publish an
 * {@link com.arbitrage.common.model.ArbitrageOpportunity}.
 *
 * <p><b>Direction:</b> All fields are calculated under the assumption that the
 * sell-side exchange has the higher best-bid and the buy-side exchange has the lower
 * best-ask. A negative {@link #grossSpread} or {@link #netSpread} means this direction
 * is unprofitable and should be discarded.
 *
 * <p><b>Scale and rounding:</b> All fields are stored at scale 8 with
 * {@code HALF_UP} rounding, matching crypto price precision (1 satoshi = 1e-8).
 */
@Value
@Builder
public class SpreadCalculationResult {

    /**
     * Price difference before fees: {@code sellBidPrice - buyAskPrice}.
     * Positive means sell exchange quotes higher than buy exchange — a candidate opportunity.
     * Negative or zero means no opportunity in this direction.
     */
    BigDecimal grossSpread;

    /**
     * Price difference after deducting taker fees on both legs:
     * {@code grossSpread - (buyAskPrice × buyFeeRate) - (sellBidPrice × sellFeeRate)}.
     *
     * <p>A positive net spread indicates a theoretically profitable trade in this direction.
     * Most detected spreads on liquid pairs (BTC-USDT) will be net-negative because typical
     * fees ($134 round-trip) dwarf typical spreads ($15).
     */
    BigDecimal netSpread;

    /**
     * Gross spread expressed in basis points: {@code (grossSpread / buyAskPrice) × 10,000}.
     * Traders reason in bps — "5 bps spread" is more intuitive than "$3.35."
     * Used by the detection engine as a noise pre-filter.
     */
    BigDecimal grossSpreadBps;

    /**
     * Net spread expressed in basis points: {@code (netSpread / buyAskPrice) × 10,000}.
     * A positive value here directly answers "is this profitable, and by how much?"
     */
    BigDecimal netSpreadBps;

    /**
     * Maximum quantity that can be arbitraged: {@code min(sellBidQuantity, buyAskQuantity)}.
     *
     * <p>Profit is bounded by the thinner side of the order book. We can only sell as many
     * units as the sell exchange's order book offers at the best bid, AND as many units
     * as the buy exchange's order book offers at the best ask.
     */
    BigDecimal arbitrageableQuantity;

    /**
     * Theoretical profit if both legs execute at quoted prices:
     * {@code netSpread × arbitrageableQuantity}.
     *
     * <p>"Theoretical" because real execution faces slippage, partial fills,
     * and price movement during the execution window — modelled in Phase 4.
     */
    BigDecimal theoreticalProfit;
}
