package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.model.SlippageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SlippageEstimator}.
 *
 * <p>Mocks {@link PriceAtExecutionLookup} so tests focus purely on slippage
 * calculation logic — not on price store retrieval (covered by Session 4.3 tests).
 * Uses a directly-instantiated {@link SimulationProperties} with {@code defaultSlippageBps=2.00}.
 */
@ExtendWith(MockitoExtension.class)
class SlippageEstimatorTest {

    private static final TradingPair BTC_USDT   = TradingPair.fromSymbol("BTC-USDT");
    private static final ExchangeId  BUY_EXCH   = ExchangeId.BINANCE;
    private static final ExchangeId  SELL_EXCH  = ExchangeId.BYBIT;
    private static final long        LATENCY_MS = 43L;

    @Mock
    private PriceAtExecutionLookup priceAtExecutionLookup;

    private SlippageEstimator estimator;

    @BeforeEach
    void setUp() {
        SimulationProperties props = new SimulationProperties();
        props.setDefaultSlippageBps(new BigDecimal("2.00"));
        estimator = new SlippageEstimator(priceAtExecutionLookup, props);
    }

    // ─── Zero slippage ────────────────────────────────────────────────────────

    @Test
    @DisplayName("prices unchanged at execution — zero slippage on both legs")
    void estimate_pricesUnchanged_returnsZeroSlippage() {
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH,  "49999.00", "50000.00"); // ask unchanged
        stubLookup(SELL_EXCH, "50100.00", "50101.00"); // bid unchanged

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("0.0000");
        assertThat(result.getSellSlippageBps()).isEqualByComparingTo("0.0000");
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo("0.0000");
        assertThat(result.isPriceDataAvailable()).isTrue();
    }

    // ─── Adverse price moves ──────────────────────────────────────────────────

    @Test
    @DisplayName("buy ask rose during execution — positive buy slippage")
    void estimate_buyPriceRose_positiveBuySlippage() {
        // Detection ask = 50000, execution ask = 50010 → 2 bps adverse
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH,  "49998.00", "50010.00"); // ask up by 10
        stubLookup(SELL_EXCH, "50100.00", "50101.00"); // bid unchanged

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        // (50010 - 50000) / 50000 * 10000 = 2.0000 bps
        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("2.0000");
        assertThat(result.getSellSlippageBps()).isEqualByComparingTo("0.0000");
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo("2.0000");
        assertThat(result.getBuyExecutionPrice()).isEqualByComparingTo("50010.00");
        assertThat(result.isPriceDataAvailable()).isTrue();
    }

    @Test
    @DisplayName("sell bid fell during execution — positive sell slippage")
    void estimate_sellBidFell_positiveSellSlippage() {
        // Detection bid = 50100, execution bid = 50090 → ~1.99 bps adverse
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH,  "49999.00", "50000.00"); // ask unchanged
        stubLookup(SELL_EXCH, "50090.00", "50091.00"); // bid down by 10

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        // (50100 - 50090) / 50100 * 10000 = 1.9960... bps
        assertThat(result.getSellSlippageBps()).isEqualByComparingTo("1.9960");
        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("0.0000");
        assertThat(result.getSellExecutionPrice()).isEqualByComparingTo("50090.00");
        assertThat(result.isPriceDataAvailable()).isTrue();
    }

    @Test
    @DisplayName("both legs move adversely — total is sum of both")
    void estimate_bothLegsAdverse_totalIsSumOfBothLegs() {
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH,  "49999.00", "50010.00"); // ask up 10 → 2 bps
        stubLookup(SELL_EXCH, "50090.00", "50091.00"); // bid down 10 → ~1.996 bps

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        BigDecimal expectedTotal = result.getBuySlippageBps().add(result.getSellSlippageBps());
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo(expectedTotal);
        assertThat(result.isPriceDataAvailable()).isTrue();
    }

    @Test
    @DisplayName("favorable price move on buy — negative buy slippage")
    void estimate_buyPriceFell_negativeBuySlippage() {
        // Detection ask = 50000, execution ask = 49990 — ask fell, we pay less
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH,  "49989.00", "49990.00"); // ask down by 10 → -2 bps
        stubLookup(SELL_EXCH, "50100.00", "50101.00");

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        // (49990 - 50000) / 50000 * 10000 = -2.0000 bps
        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("-2.0000");
        assertThat(result.isPriceDataAvailable()).isTrue();
    }

    // ─── Fallback behaviour ───────────────────────────────────────────────────

    @Test
    @DisplayName("buy store empty — falls back to detection prices and defaultSlippageBps")
    void estimate_buyStoreEmpty_usesFallback() {
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        when(priceAtExecutionLookup.findClosestBefore(eq(BUY_EXCH), any(), any()))
                .thenReturn(Optional.empty());
        stubLookup(SELL_EXCH, "50100.00", "50101.00");

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        assertThat(result.isPriceDataAvailable()).isFalse();
        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("2.00");
        assertThat(result.getSellSlippageBps()).isEqualByComparingTo("2.00");
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo("4.00");
        assertThat(result.getBuyExecutionPrice()).isEqualByComparingTo(opp.getBuyPrice());
        assertThat(result.getSellExecutionPrice()).isEqualByComparingTo(opp.getSellPrice());
    }

    @Test
    @DisplayName("sell store empty — falls back to detection prices and defaultSlippageBps")
    void estimate_sellStoreEmpty_usesFallback() {
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        stubLookup(BUY_EXCH, "49999.00", "50000.00");
        when(priceAtExecutionLookup.findClosestBefore(eq(SELL_EXCH), any(), any()))
                .thenReturn(Optional.empty());

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        assertThat(result.isPriceDataAvailable()).isFalse();
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo("4.00");
    }

    @Test
    @DisplayName("both stores empty — falls back to defaultSlippageBps per leg")
    void estimate_bothStoresEmpty_usesFallbackWithDefaultSlippage() {
        ArbitrageOpportunity opp = buildOpportunity("50000.00", "50100.00");
        when(priceAtExecutionLookup.findClosestBefore(any(), any(), any()))
                .thenReturn(Optional.empty());

        SlippageResult result = estimator.estimate(opp, LATENCY_MS);

        assertThat(result.isPriceDataAvailable()).isFalse();
        assertThat(result.getBuySlippageBps()).isEqualByComparingTo("2.00");
        assertThat(result.getSellSlippageBps()).isEqualByComparingTo("2.00");
        assertThat(result.getTotalSlippageBps()).isEqualByComparingTo("4.00");
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private ArbitrageOpportunity buildOpportunity(String buyAsk, String sellBid) {
        BigDecimal buyPrice  = new BigDecimal(buyAsk);
        BigDecimal sellPrice = new BigDecimal(sellBid);
        BigDecimal gross     = sellPrice.subtract(buyPrice);
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(BTC_USDT)
                .buyExchange(BUY_EXCH)
                .sellExchange(SELL_EXCH)
                .buyPrice(buyPrice)
                .buyQuantity(BigDecimal.ONE)
                .buyFeeRate(new BigDecimal("0.0010"))
                .sellPrice(sellPrice)
                .sellQuantity(BigDecimal.ONE)
                .sellFeeRate(new BigDecimal("0.0010"))
                .grossSpread(gross)
                .netSpread(gross)
                .grossSpreadBps(BigDecimal.TEN)
                .netSpreadBps(BigDecimal.ONE)
                .arbitrageableQuantity(BigDecimal.ONE)
                .theoreticalProfit(gross)
                .status(OpportunityStatus.CLOSED)
                .detectionTimestamp(Instant.now().minusMillis(100))
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(System.nanoTime())
                .closedNanoTime(System.nanoTime())
                .peakNetSpread(gross)
                .averageNetSpread(gross)
                .totalDurationMs(50L)
                .updateCount(5L)
                .build();
    }

    private void stubLookup(ExchangeId exchange, String bidPrice, String askPrice) {
        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(exchange)
                .tradingPair(BTC_USDT)
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(askPrice))
                .bestBidQuantity(BigDecimal.ONE)
                .bestAskQuantity(BigDecimal.ONE)
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
        when(priceAtExecutionLookup.findClosestBefore(eq(exchange), any(), any()))
                .thenReturn(Optional.of(tick));
    }
}
