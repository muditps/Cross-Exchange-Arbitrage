package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.config.FeeConfiguration;
import com.arbitrage.detection.model.PriceState;
import com.arbitrage.detection.service.LatencyRecorder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ArbitrageDetectionEngine#compareAllDirections}.
 *
 * <p>Tests the orchestration logic: staleness gate, direction enumeration, noise filter,
 * profitability filter, and correct assembly of {@link ArbitrageOpportunity}.
 * {@link SpreadCalculator} is NOT mocked — using a real instance proves the full math.
 * {@link DetectionProperties} and {@link StalenessFilter} are mocked to control behaviour.
 *
 * <p>Default {@code @BeforeEach} stubs: all prices are fresh ({@code isStale → false}),
 * min spread threshold is 10 bps. Both stubs are {@code lenient()} so tests that return
 * early (empty map, single exchange) don't trigger {@code UnnecessaryStubbingException}.
 *
 * <p>The {@code @KafkaListener} path ({@link ArbitrageDetectionEngine#onNormalisedTick})
 * is verified in the Phase 3 integration test (Session 3.8) with a live Kafka container.
 */
@ExtendWith(MockitoExtension.class)
class ArbitrageDetectionEngineTest {

    @Mock
    private PriceStateService priceStateService;

    @Mock
    private DetectionProperties detectionProperties;

    @Mock
    private StalenessFilter stalenessFilter;

    @Mock
    private OpportunityTracker opportunityTracker;

    @Mock
    private OpportunityKafkaPublisher opportunityPublisher;

    @Mock
    private DetectionMetrics detectionMetrics;

    @Mock
    private FeeConfiguration feeConfiguration;

    @Mock
    private LatencyRecorder latencyRecorder;

    private SpreadCalculator spreadCalculator;
    private ArbitrageDetectionEngine engine;

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");

    @BeforeEach
    void setUp() {
        spreadCalculator = new SpreadCalculator();
        engine = new ArbitrageDetectionEngine(
                priceStateService, spreadCalculator, detectionProperties, stalenessFilter,
                opportunityTracker, opportunityPublisher, detectionMetrics, feeConfiguration,
                latencyRecorder);
        // lenient: early-return tests (empty map, single exchange) don't call these
        lenient().when(detectionProperties.getMinSpreadBps()).thenReturn(10);
        lenient().when(stalenessFilter.isStale(any())).thenReturn(false);
        lenient().when(feeConfiguration.getTakerFeeRate(any())).thenReturn(new BigDecimal("0.0010"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Profitability filtering
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("profitable direction is included in result list")
    void compareAllDirections_profitableSpread_returnsOpportunity() {
        // Binance bid=$65200, KuCoin ask=$65000 → grossSpread=$200, netSpread=$69.80 (positive)
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).hasSize(1);
        ArbitrageOpportunity opp = result.get(0);
        assertThat(opp.getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(opp.getBuyExchange()).isEqualTo(ExchangeId.KUCOIN);
        assertThat(opp.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();
        assertThat(opp.getStatus()).isEqualTo(OpportunityStatus.DETECTED);
    }

    @Test
    @DisplayName("net-negative spread (fees exceed gross spread) is excluded")
    void compareAllDirections_feesConsumeSpread_returnsEmpty() {
        // $10 gross spread, ~$130 in fees → netSpread deeply negative
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65010.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Noise filter (minSpreadBps)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("gross spread below minSpreadBps threshold is rejected before fee computation")
    void compareAllDirections_grossSpreadBelowMinBps_returnsEmpty() {
        // grossSpreadBps = (5 / 65000) × 10000 ≈ 0.77 bps → below 10 bps threshold
        // Even with zero fees this would be rejected
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65005.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("exact minimum spread bps threshold: spread at threshold is checked; 0 bps is rejected")
    void compareAllDirections_zeroGrossSpread_rejectedByNoiseFilter() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Insufficient exchange coverage
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("single exchange in price map returns empty — need at least 2 to compare")
    void compareAllDirections_singleExchangePrice_returnsEmpty() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("empty price map returns empty")
    void compareAllDirections_emptyPriceMap_returnsEmpty() {
        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, Map.of());

        assertThat(result).isEmpty();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Direction independence
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Exchange A has a higher bid, Exchange B has a higher ask (crossed market).
     * Direction A→B: buy on B (ask 65000), sell on A (bid 65200) → netSpread positive.
     * Direction B→A: buy on A (ask 65050), sell on B (bid 65150) → netSpread negative.
     * Only one direction should produce an opportunity.
     */
    @Test
    @DisplayName("both directions checked independently — only profitable direction returned")
    void compareAllDirections_onlyOneDirectionProfitable_returnsThatOne() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        // Exchange A: bid=65200, ask=65050
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65050.00", "1.00000000", "1.00000000"));
        // Exchange B: bid=65150, ask=65000
        prices.put(ExchangeId.KUCOIN,  priceState("65150.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        // Direction: sell on BINANCE (bid 65200), buy on KUCOIN (ask 65000) → netSpread = 69.80 ✓
        // Direction: sell on KUCOIN  (bid 65150), buy on BINANCE (ask 65050) → netSpread = -30.20 ✗
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(result.get(0).getBuyExchange()).isEqualTo(ExchangeId.KUCOIN);
    }

    @Test
    @DisplayName("all three exchanges present — all 6 directions evaluated, only profitable ones returned")
    void compareAllDirections_threeExchanges_allDirectionsEvaluated() {
        // BINANCE: high bid (65200) — profitable to sell here vs both other exchanges
        // BYBIT/KUCOIN: low bid (65010) — only $10 gross spread against each other → below 10 bps threshold
        // BINANCE ask (65250) is above both competitors' bids — reverse directions are unprofitable
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65250.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.BYBIT,   priceState("65010.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65010.00", "65000.00", "1.00000000", "1.00000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        // Sell BINANCE (65200) / Buy BYBIT (65000)   → grossSpread=200, netSpread=69.80 ✓
        // Sell BINANCE (65200) / Buy KUCOIN (65000)  → grossSpread=200, netSpread=69.80 ✓
        // Sell BYBIT (65010) / Buy BINANCE (65250)   → grossSpread=-240 (negative, ✗)
        // Sell BYBIT (65010) / Buy KUCOIN (65000)    → grossSpreadBps≈1.5 bps < 10 bps threshold ✗
        // Sell KUCOIN (65010) / Buy BINANCE (65250)  → grossSpread=-240 (negative, ✗)
        // Sell KUCOIN (65010) / Buy BYBIT (65000)    → grossSpreadBps≈1.5 bps < 10 bps threshold ✗
        assertThat(result).hasSize(2);
        assertThat(result).allSatisfy(opp -> {
            assertThat(opp.getSellExchange()).isEqualTo(ExchangeId.BINANCE);
            assertThat(opp.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Opportunity field correctness
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("opportunity fields are populated correctly from price states and calculation result")
    void compareAllDirections_profitableSpread_opportunityFieldsAreCorrect() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65000.00", "2.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "0.50000000"));

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).hasSize(1);
        ArbitrageOpportunity opp = result.get(0);

        // Identity and lifecycle
        assertThat(opp.getId()).isNotNull();
        assertThat(opp.getTradingPair()).isEqualTo(BTC_USDT);
        assertThat(opp.getStatus()).isEqualTo(OpportunityStatus.DETECTED);
        assertThat(opp.getDetectionTimestamp()).isNotNull();
        assertThat(opp.getDetectedNanoTime()).isPositive();
        assertThat(opp.getClosedNanoTime()).isZero();
        assertThat(opp.getTotalDurationMs()).isZero();

        // Buy side: buying on KuCoin at its best ask
        assertThat(opp.getBuyExchange()).isEqualTo(ExchangeId.KUCOIN);
        assertThat(opp.getBuyPrice()).isEqualByComparingTo(new BigDecimal("65000.00"));
        assertThat(opp.getBuyQuantity()).isEqualByComparingTo(new BigDecimal("0.50000000"));
        assertThat(opp.getBuyFeeRate()).isEqualByComparingTo(ExchangeId.KUCOIN.getDefaultTakerFeeRate());

        // Sell side: selling on Binance at its best bid
        assertThat(opp.getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(opp.getSellPrice()).isEqualByComparingTo(new BigDecimal("65200.00"));
        assertThat(opp.getSellQuantity()).isEqualByComparingTo(new BigDecimal("2.00000000"));
        assertThat(opp.getSellFeeRate()).isEqualByComparingTo(ExchangeId.BINANCE.getDefaultTakerFeeRate());

        // arbitrageableQuantity = min(sellBidQty=2.0, buyAskQty=0.5) = 0.5
        assertThat(opp.getArbitrageableQuantity()).isEqualByComparingTo(new BigDecimal("0.50000000"));

        // netSpread = 69.80, peakNetSpread and averageNetSpread initialised to same value
        assertThat(opp.getNetSpread().compareTo(BigDecimal.ZERO)).isPositive();
        assertThat(opp.getPeakNetSpread()).isEqualByComparingTo(opp.getNetSpread());
        assertThat(opp.getAverageNetSpread()).isEqualByComparingTo(opp.getNetSpread());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Staleness gate
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("stale sell-side price causes direction to be skipped and recordStaleSkip called")
    void compareAllDirections_staleSellSide_directionSkipped() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        PriceState staleState = priceState("65200.00", "65000.00", "1.00000000", "1.00000000");
        PriceState freshState = priceState("65000.00", "65000.00", "1.00000000", "1.00000000");
        prices.put(ExchangeId.BINANCE, staleState);
        prices.put(ExchangeId.KUCOIN,  freshState);

        // BINANCE sell-side is stale; KUCOIN sell-side is fresh but KuCoin bid < Binance ask → no profit
        when(stalenessFilter.isStale(staleState)).thenReturn(true);
        when(stalenessFilter.isStale(freshState)).thenReturn(false);

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
        // staleState (BINANCE) is encountered twice: once as sell-side (BINANCE→KUCOIN),
        // once as buy-side (KUCOIN→BINANCE). Both directions are correctly skipped.
        verify(stalenessFilter, times(2)).recordStaleSkip(ExchangeId.BINANCE, BTC_USDT, staleState);
    }

    @Test
    @DisplayName("stale buy-side price causes direction to be skipped and recordStaleSkip called")
    void compareAllDirections_staleBuySide_directionSkipped() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        PriceState sellState  = priceState("65200.00", "65000.00", "1.00000000", "1.00000000");
        PriceState staleState = priceState("65000.00", "65000.00", "1.00000000", "1.00000000");
        prices.put(ExchangeId.BINANCE, sellState);
        prices.put(ExchangeId.KUCOIN,  staleState);

        when(stalenessFilter.isStale(sellState)).thenReturn(false);
        when(stalenessFilter.isStale(staleState)).thenReturn(true);

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).isEmpty();
        // staleState (KUCOIN) encountered twice: once as sell-side (KUCOIN→BINANCE),
        // once as buy-side (BINANCE→KUCOIN). Both directions correctly skipped.
        verify(stalenessFilter, times(2)).recordStaleSkip(ExchangeId.KUCOIN, BTC_USDT, staleState);
    }

    @Test
    @DisplayName("fresh prices are not affected by staleness gate — profitable direction still returned")
    void compareAllDirections_freshPrices_stalenessGateDoesNotBlock() {
        Map<ExchangeId, PriceState> prices = new EnumMap<>(ExchangeId.class);
        prices.put(ExchangeId.BINANCE, priceState("65200.00", "65000.00", "1.00000000", "1.00000000"));
        prices.put(ExchangeId.KUCOIN,  priceState("65000.00", "65000.00", "1.00000000", "1.00000000"));
        // default setUp stub: stalenessFilter.isStale(any()) → false

        List<ArbitrageOpportunity> result = engine.compareAllDirections(BTC_USDT, prices);

        assertThat(result).hasSize(1);
        verify(stalenessFilter, never()).recordStaleSkip(any(), any(), any());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

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
}
