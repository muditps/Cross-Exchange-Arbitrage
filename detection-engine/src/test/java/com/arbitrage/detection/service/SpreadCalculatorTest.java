package com.arbitrage.detection.service;

import com.arbitrage.detection.model.PriceState;
import com.arbitrage.detection.model.SpreadCalculationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SpreadCalculator}.
 *
 * <p>All 8 test cases mandated by the development plan are covered, plus edge-case
 * verification that both directions between the same two exchanges produce independent results.
 *
 * <p>No Spring context is needed — {@link SpreadCalculator} is a pure stateless component.
 *
 * <p><b>BigDecimal comparisons:</b> All assertions use {@link BigDecimal#compareTo} instead
 * of {@code equals} to avoid scale mismatch failures ({@code 69.8} vs {@code 69.80000000}).
 * The custom {@code assertBdEquals} helper enforces this throughout.
 */
class SpreadCalculatorTest {

    private SpreadCalculator calculator;

    /** 0.10% taker fee — the industry standard for the three exchanges in scope. */
    private static final BigDecimal FEE_0_10_PCT = new BigDecimal("0.0010");

    /** 0.00% fee — used in edge-case tests to isolate gross spread. */
    private static final BigDecimal FEE_ZERO = BigDecimal.ZERO;

    @BeforeEach
    void setUp() {
        calculator = new SpreadCalculator();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1 — Normal case: clear positive spread → correct profit
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario: sell on Exchange A (bid $65,200), buy on Exchange B (ask $65,000).
     * grossSpread = $200. buyCost = $65. sellCost = $65.20. netSpread = $69.80.
     * Both exchanges have 1.0 BTC available → profit = $69.80.
     */
    @Test
    @DisplayName("positive spread with fees leaves positive net spread and correct profit")
    void calculate_clearPositiveSpread_returnsCorrectNetSpreadAndProfit() {
        PriceState sellState = priceState("65200.00", "65000.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64900.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        // grossSpread = 65200.00 - 65000.00 = 200.00
        assertBdEquals("200.00000000", result.getGrossSpread());

        // buyCost = 65000.00 × 0.0010 = 65.00, sellCost = 65200.00 × 0.0010 = 65.20
        // netSpread = 200.00 - 65.00 - 65.20 = 69.80
        assertBdEquals("69.80000000", result.getNetSpread());

        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();

        // arbitrageableQty = min(1.0, 1.0) = 1.0
        assertBdEquals("1.00000000", result.getArbitrageableQuantity());

        // theoreticalProfit = 69.80 × 1.0 = 69.80
        assertBdEquals("69.80000000", result.getTheoreticalProfit());

        // grossSpreadBps = (200 / 65000) × 10000 = 30.76923077
        assertBdEquals("30.76923077", result.getGrossSpreadBps());

        // netSpreadBps = (69.80 / 65000) × 10000 = 10.73846154
        assertBdEquals("10.73846154", result.getNetSpreadBps());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2 — Fees consume spread: gross positive, net negative
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario: sell at $65,010, buy at $65,000 — only $10 gross spread.
     * Fees total $130.01 (buyCost $65.00 + sellCost $65.01) → net = -$120.01.
     * This is the common case for liquid pairs: fees dwarf typical spreads.
     */
    @Test
    @DisplayName("gross positive spread consumed by fees produces negative net spread")
    void calculate_feesConsumeSpread_netSpreadIsNegative() {
        PriceState sellState = priceState("65010.00", "64800.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64700.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        // grossSpread = 65010.00 - 65000.00 = 10.00
        assertBdEquals("10.00000000", result.getGrossSpread());
        assertThat(result.getGrossSpread().compareTo(BigDecimal.ZERO)).isPositive();

        // buyCost = 65.00, sellCost = 65.01, netSpread = 10 - 65 - 65.01 = -120.01
        assertBdEquals("-120.01000000", result.getNetSpread());
        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isNegative();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3 — Zero spread: identical best prices
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario: sell bid and buy ask are both $65,000. grossSpread = $0.
     * Even with zero gross spread, fees make netSpread deeply negative.
     * (If we used zero fees, netSpread would be exactly zero — also not an opportunity.)
     */
    @Test
    @DisplayName("zero gross spread produces zero grossSpreadBps and deeply negative net spread")
    void calculate_zeroSpread_grossSpreadIsZero() {
        PriceState sellState = priceState("65000.00", "64800.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64700.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        assertBdEquals("0.00000000", result.getGrossSpread());
        assertBdEquals("0.00000000", result.getGrossSpreadBps());
        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isNegative();

        // With zero fees the result should be exactly zero
        SpreadCalculationResult zeroFeeResult = calculator.calculate(sellState, FEE_ZERO, buyState, FEE_ZERO);
        assertBdEquals("0.00000000", zeroFeeResult.getNetSpread());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4 — Negative spread: sell bid < buy ask
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Scenario: sell bid ($64,990) is below buy ask ($65,000). Buying for more than
     * we can sell for is never profitable. grossSpread is negative, netSpread is even more so.
     */
    @Test
    @DisplayName("sell bid below buy ask produces negative gross spread")
    void calculate_negativeCrossedSpread_grossAndNetSpreadAreNegative() {
        PriceState sellState = priceState("64990.00", "64800.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64700.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        // grossSpread = 64990 - 65000 = -10.00
        assertBdEquals("-10.00000000", result.getGrossSpread());
        assertThat(result.getGrossSpread().compareTo(BigDecimal.ZERO)).isNegative();
        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isNegative();
        assertThat(result.getGrossSpreadBps().compareTo(BigDecimal.ZERO)).isNegative();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5 — Precision at 8 decimal places (BigDecimal vs double)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * KEY CONCEPT: {@code double} cannot represent prices at 8 decimal places without
     * rounding error. This test verifies that a 1-satoshi ($0.00000001) price difference
     * is preserved exactly by {@code BigDecimal} — a {@code double} would round it to zero.
     */
    @Test
    @DisplayName("price difference of 1 satoshi (0.00000001) is preserved exactly")
    void calculate_oneSatoshiDifference_precisionPreserved() {
        // Sell at one satoshi above the buy price
        PriceState sellState = priceState("65000.00000001", "64999.00000000", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64998.00000000", "65000.00000000", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_ZERO, buyState, FEE_ZERO);

        // With zero fees, netSpread = grossSpread = 0.00000001 exactly
        assertBdEquals("0.00000001", result.getGrossSpread());
        assertBdEquals("0.00000001", result.getNetSpread());

        // double would lose this: 65000.00000001 - 65000.00000000 == 0.0 in IEEE 754
        // BigDecimal preserves it because it stores the exact decimal representation
        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6 — Both directions produce different results
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Two exchanges with different bid/ask prices. Each direction (A sells / B buys, and
     * B sells / A buys) produces a different gross spread and different net spread.
     * The engine must check both directions independently.
     */
    @Test
    @DisplayName("direction A→B and direction B→A produce different spread values")
    void calculate_bothDirections_produceDifferentResults() {
        // Exchange A: bid=65200, ask=65050
        PriceState stateA = priceState("65200.00", "65050.00", "1.00000000", "1.00000000");
        // Exchange B: bid=65150, ask=65000
        PriceState stateB = priceState("65150.00", "65000.00", "1.00000000", "1.00000000");

        // Direction 1: sell on A (bid 65200), buy on B (ask 65000)
        // grossSpread = 65200 - 65000 = 200.00
        SpreadCalculationResult direction1 = calculator.calculate(stateA, FEE_0_10_PCT, stateB, FEE_0_10_PCT);

        // Direction 2: sell on B (bid 65150), buy on A (ask 65050)
        // grossSpread = 65150 - 65050 = 100.00
        SpreadCalculationResult direction2 = calculator.calculate(stateB, FEE_0_10_PCT, stateA, FEE_0_10_PCT);

        assertBdEquals("200.00000000", direction1.getGrossSpread());
        assertBdEquals("100.00000000", direction2.getGrossSpread());

        // netSpread direction1 = 200 - 65 - 65.20 = 69.80 (positive)
        assertBdEquals("69.80000000", direction1.getNetSpread());
        assertThat(direction1.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();

        // netSpread direction2 = 100 - 65.05 - 65.15 = -30.20 (negative — not profitable)
        assertBdEquals("-30.20000000", direction2.getNetSpread());
        assertThat(direction2.getNetSpread().compareTo(BigDecimal.ZERO)).isNegative();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7 — Quantity bounded by the thinner order book side
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * The sell exchange has 2.0 BTC at its bid, the buy exchange only 0.5 BTC at its ask.
     * We can only execute 0.5 BTC — profit is bounded by min(bidQty, askQty).
     */
    @Test
    @DisplayName("arbitrageableQuantity is bounded by the thinner order book side")
    void calculate_unequalQuantities_usesMinimum() {
        PriceState sellState = priceState("65200.00", "65000.00", "2.00000000", "1.50000000");
        PriceState buyState  = priceState("64900.00", "65000.00", "1.50000000", "0.50000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        // min(sellBidQty=2.0, buyAskQty=0.5) = 0.5
        assertBdEquals("0.50000000", result.getArbitrageableQuantity());

        // theoreticalProfit = 69.80 × 0.5 = 34.90
        assertBdEquals("34.90000000", result.getTheoreticalProfit());
    }

    @Test
    @DisplayName("arbitrageableQuantity uses sell bid quantity when it is the smaller side")
    void calculate_sellBidQtySmaller_usesSellBidQty() {
        PriceState sellState = priceState("65200.00", "65000.00", "0.30000000", "1.00000000");
        PriceState buyState  = priceState("64900.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_0_10_PCT, buyState, FEE_0_10_PCT);

        // min(sellBidQty=0.3, buyAskQty=1.0) = 0.3
        assertBdEquals("0.30000000", result.getArbitrageableQuantity());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 8 — v1.1 canonical example: confirms formula exactly as designed
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * From Development Plan Supplement v1.1:
     * Binance bid $67,250.50, KuCoin ask $67,235.20, both 0.10% taker fees.
     *
     * <p>Expected:
     * <pre>
     *   grossSpread = 67250.50 - 67235.20 = 15.30
     *   buyCost     = 67235.20 × 0.0010   = 67.23520000
     *   sellCost    = 67250.50 × 0.0010   = 67.25050000
     *   netSpread   = 15.30 - 67.23520000 - 67.25050000 = -119.18570000
     * </pre>
     * Result: NOT profitable. Fees ($134.49) dwarf the gross spread ($15.30).
     * Rounded to 2dp this is -$119.19, as stated in the plan.
     *
     * <p><b>Why this test matters:</b> This is the expected real-world scenario for major
     * liquid pairs like BTC-USDT. Markets are efficient — the spread is smaller than the
     * cost of capturing it. The system should correctly identify this as NOT profitable,
     * avoiding false positives that would destroy credibility.
     */
    @Test
    @DisplayName("v1.1 canonical example: Binance bid 67250.50 / KuCoin ask 67235.20 → net -119.18570000")
    void calculate_v11CanonicalExample_netSpreadIsNegative() {
        // Sell on Binance (higher bid), buy on KuCoin (lower ask)
        PriceState binanceSellState = priceState("67250.50", "67100.00", "1.00000000", "1.00000000");
        PriceState kucoinBuyState   = priceState("67100.00", "67235.20", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(
                binanceSellState, FEE_0_10_PCT,
                kucoinBuyState,   FEE_0_10_PCT);

        // grossSpread = 67250.50 - 67235.20 = 15.30
        assertBdEquals("15.30000000", result.getGrossSpread());
        assertThat(result.getGrossSpread().compareTo(BigDecimal.ZERO)).isPositive();

        // netSpread = 15.30 - 67.23520000 - 67.25050000 = -119.18570000
        assertBdEquals("-119.18570000", result.getNetSpread());
        assertThat(result.getNetSpread().compareTo(BigDecimal.ZERO)).isNegative();

        // Rounded to 2dp this is the -$119.19 from the dev plan
        assertThat(result.getNetSpread().setScale(2, SpreadCalculator.ROUNDING))
                .isEqualByComparingTo(new BigDecimal("-119.19"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Additional edge cases
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("zero fees with positive gross spread: netSpread equals grossSpread exactly")
    void calculate_zeroFees_netSpreadEqualsGrossSpread() {
        PriceState sellState = priceState("65200.00", "65000.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64900.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_ZERO, buyState, FEE_ZERO);

        assertBdEquals("200.00000000", result.getGrossSpread());
        assertBdEquals("200.00000000", result.getNetSpread());
        assertBdEquals("200.00000000", result.getTheoreticalProfit()); // qty = 1.0
    }

    @Test
    @DisplayName("grossSpreadBps calculated relative to buy ask price, not sell bid")
    void calculate_bpsRelativeToBuyAskPrice() {
        // sell bid=65200, buy ask=65000
        // grossSpreadBps = (200 / 65000) × 10000 = 30.76923077
        PriceState sellState = priceState("65200.00", "65000.00", "1.00000000", "1.00000000");
        PriceState buyState  = priceState("64900.00", "65000.00", "1.00000000", "1.00000000");

        SpreadCalculationResult result = calculator.calculate(sellState, FEE_ZERO, buyState, FEE_ZERO);

        // (200 / 65000) × 10000 = 30.76923076... rounds to 30.76923077
        assertBdEquals("30.76923077", result.getGrossSpreadBps());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Builds a minimal {@link PriceState} for testing.
     *
     * @param bid    best bid price (we sell here)
     * @param ask    best ask price (we buy here)
     * @param bidQty quantity at best bid
     * @param askQty quantity at best ask
     */
    private PriceState priceState(String bid, String ask, String bidQty, String askQty) {
        return PriceState.builder()
                .bestBidPrice(new BigDecimal(bid))
                .bestAskPrice(new BigDecimal(ask))
                .bestBidQuantity(new BigDecimal(bidQty))
                .bestAskQuantity(new BigDecimal(askQty))
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }

    /**
     * Asserts two BigDecimal values are equal by value, ignoring scale differences.
     *
     * <p>{@code BigDecimal.equals} considers scale: {@code 69.8 != 69.80000000}.
     * {@code BigDecimal.compareTo} considers only numeric value, which is what we want.
     *
     * @param expected string representation of the expected value
     * @param actual   the actual BigDecimal from the calculator
     */
    private void assertBdEquals(String expected, BigDecimal actual) {
        BigDecimal expectedBd = new BigDecimal(expected);
        assertThat(actual.compareTo(expectedBd))
                .as("Expected %s but got %s", expected, actual.toPlainString())
                .isZero();
    }
}
