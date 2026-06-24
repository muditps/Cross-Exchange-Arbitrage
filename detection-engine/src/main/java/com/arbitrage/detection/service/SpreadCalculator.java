package com.arbitrage.detection.service;

import com.arbitrage.detection.model.PriceState;
import com.arbitrage.detection.model.SpreadCalculationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Calculates the spread and theoretical profit for a single directional arbitrage trade.
 *
 * <p>This is a pure, stateless calculation component — no Redis, no Kafka, no external state.
 * It models one trade direction: buy the asset on the buy-side exchange (at its best ask),
 * and simultaneously sell it on the sell-side exchange (at its best bid).
 *
 * <p><b>Formula (Pre-Funded Arbitrage Model):</b>
 * <pre>
 *   grossSpread         = sellBidPrice - buyAskPrice
 *   buyCost             = buyAskPrice  × buyFeeRate
 *   sellCost            = sellBidPrice × sellFeeRate
 *   netSpread           = grossSpread  - buyCost - sellCost
 *   arbitrageableQty    = min(sellBidQuantity, buyAskQuantity)
 *   theoreticalProfit   = netSpread × arbitrageableQty
 *   grossSpreadBps      = (grossSpread / buyAskPrice) × 10,000
 *   netSpreadBps        = (netSpread   / buyAskPrice) × 10,000
 * </pre>
 *
 * <p><b>Why "pre-funded"?</b> We assume the trading account already holds balances on all
 * exchanges simultaneously. Both legs (buy and sell) are sent at the same instant — no asset
 * transfer per trade. Withdrawal fees only apply during periodic rebalancing (daily/weekly).
 * This is how real arbitrage firms operate.
 *
 * <p><b>Why BigDecimal?</b> {@code double} cannot represent 0.1 or 0.2 exactly. With crypto
 * prices at 8 decimal places and spreads often at 0.01%, a {@code 1e-10} rounding error
 * flips comparisons. {@code BigDecimal} with explicit scale (8) and {@code HALF_UP} rounding
 * prevents this class of bugs entirely.
 *
 * <p><b>What the caller must do:</b> This method always returns a result regardless of
 * profitability. A negative {@code netSpread} is a valid result (not an error). The caller
 * ({@link ArbitrageDetectionEngine}) inspects the result and discards unprofitable directions.
 *
 * <p><b>Both directions must be checked explicitly:</b> This method calculates ONE direction
 * (sell on A, buy on B). The reverse direction (sell on B, buy on A) requires a separate
 * call with swapped arguments.
 */
@Component
@Slf4j
public class SpreadCalculator {

    /** Scale applied to all intermediate and final BigDecimal calculations. Matches crypto precision. */
    static final int SCALE = 8;

    /** Rounding mode for all BigDecimal operations. HALF_UP is standard for financial calculations. */
    static final RoundingMode ROUNDING = RoundingMode.HALF_UP;

    /** Multiplier used to convert a fractional spread to basis points (1 bps = 0.01%). */
    private static final BigDecimal BPS_MULTIPLIER = new BigDecimal("10000");

    /**
     * Calculates spread and profit for one trade direction: buy on the buy-side exchange,
     * sell on the sell-side exchange.
     *
     * <p>This method makes no judgement about profitability. A negative {@code netSpread}
     * or {@code theoreticalProfit} is a valid result — it simply means this direction is
     * unprofitable and the caller should discard it.
     *
     * @param sellState   price state of the exchange where we intend to sell (higher bid expected)
     * @param sellFeeRate taker fee rate of the sell-side exchange (e.g., {@code 0.0010} for 0.10%)
     * @param buyState    price state of the exchange where we intend to buy (lower ask expected)
     * @param buyFeeRate  taker fee rate of the buy-side exchange (e.g., {@code 0.0010} for 0.10%)
     * @return the fully computed {@link SpreadCalculationResult} — never null
     */
    public SpreadCalculationResult calculate(
            PriceState sellState, BigDecimal sellFeeRate,
            PriceState buyState,  BigDecimal buyFeeRate) {

        BigDecimal sellBidPrice   = sellState.getBestBidPrice();
        BigDecimal buyAskPrice    = buyState.getBestAskPrice();
        BigDecimal sellBidQty     = sellState.getBestBidQuantity();
        BigDecimal buyAskQty      = buyState.getBestAskQuantity();

        // Step 1: Gross spread — what we make before fees
        BigDecimal grossSpread = sellBidPrice
                .subtract(buyAskPrice)
                .setScale(SCALE, ROUNDING);

        // Step 2: Fee costs — what we pay to execute both legs (taker fees on market orders)
        BigDecimal buyCost  = buyAskPrice.multiply(buyFeeRate).setScale(SCALE, ROUNDING);
        BigDecimal sellCost = sellBidPrice.multiply(sellFeeRate).setScale(SCALE, ROUNDING);

        // Step 3: Net spread — what we actually keep
        BigDecimal netSpread = grossSpread
                .subtract(buyCost)
                .subtract(sellCost)
                .setScale(SCALE, ROUNDING);

        // Step 4: Quantity — bounded by the thinner side of the order book
        BigDecimal arbitrageableQuantity = sellBidQty.min(buyAskQty)
                .setScale(SCALE, ROUNDING);

        // Step 5: Theoretical profit — net spread scaled by executable quantity
        BigDecimal theoreticalProfit = netSpread
                .multiply(arbitrageableQuantity)
                .setScale(SCALE, ROUNDING);

        // Step 6: Spread in basis points — multiply FIRST then divide to preserve decimal precision.
        // divide-then-multiply would lose up to 1e-8 precision in the intermediate quotient,
        // causing results like 30.76920000 instead of the correct 30.76923077.
        BigDecimal grossSpreadBps = grossSpread
                .multiply(BPS_MULTIPLIER)
                .divide(buyAskPrice, SCALE, ROUNDING);

        BigDecimal netSpreadBps = netSpread
                .multiply(BPS_MULTIPLIER)
                .divide(buyAskPrice, SCALE, ROUNDING);

        return SpreadCalculationResult.builder()
                .grossSpread(grossSpread)
                .netSpread(netSpread)
                .grossSpreadBps(grossSpreadBps)
                .netSpreadBps(netSpreadBps)
                .arbitrageableQuantity(arbitrageableQuantity)
                .theoreticalProfit(theoreticalProfit)
                .build();
    }
}
