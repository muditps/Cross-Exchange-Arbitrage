package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.simulator.config.SimulationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link HistoricalPriceStore}.
 *
 * <p>Uses a directly-instantiated {@link SimulationProperties} — no Spring context needed.
 * Eviction tests configure a 1-second window to avoid sleeping in tests; stale ticks
 * are given timestamps well outside the window ({@code Instant.now().minusSeconds(5)}).
 */
class HistoricalPriceStoreTest {

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");
    private static final TradingPair ETH_USDT = TradingPair.fromSymbol("ETH-USDT");

    private HistoricalPriceStore store;

    @BeforeEach
    void setUp() {
        SimulationProperties props = new SimulationProperties();
        props.setHistoricalWindowSeconds(1); // 1-second window: stale ticks use minus-5s
        store = new HistoricalPriceStore(props);
    }

    // ─── Basic storage ────────────────────────────────────────────────────────

    @Test
    @DisplayName("recordTick stores a single tick retrievable via getSnapshot")
    void recordTick_storesSingleTick() {
        NormalisedTick tick = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", Instant.now());

        store.recordTick(tick);

        List<NormalisedTick> snapshot = store.getSnapshot(ExchangeId.BINANCE, BTC_USDT);
        assertThat(snapshot).hasSize(1).containsExactly(tick);
    }

    @Test
    @DisplayName("recordTick stores multiple ticks in insertion order (oldest first)")
    void recordTick_storesMultipleTicksInOrder() {
        Instant t1 = Instant.now().minusMillis(200);
        Instant t2 = Instant.now().minusMillis(100);
        Instant t3 = Instant.now();
        NormalisedTick tick1 = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", t1);
        NormalisedTick tick2 = buildTick(ExchangeId.BINANCE, BTC_USDT, "50010", t2);
        NormalisedTick tick3 = buildTick(ExchangeId.BINANCE, BTC_USDT, "50020", t3);

        store.recordTick(tick1);
        store.recordTick(tick2);
        store.recordTick(tick3);

        List<NormalisedTick> snapshot = store.getSnapshot(ExchangeId.BINANCE, BTC_USDT);
        assertThat(snapshot).containsExactly(tick1, tick2, tick3);
    }

    // ─── Eviction ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("recordTick evicts stale ticks (older than window) on write")
    void recordTick_evictsTicksOlderThanWindow() {
        // 1-second window: minus-5s is far outside
        NormalisedTick staleTick  = buildTick(ExchangeId.BINANCE, BTC_USDT, "49000", Instant.now().minusSeconds(5));
        NormalisedTick freshTick  = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", Instant.now());

        store.recordTick(staleTick);
        store.recordTick(freshTick); // triggers eviction of staleTick

        List<NormalisedTick> snapshot = store.getSnapshot(ExchangeId.BINANCE, BTC_USDT);
        assertThat(snapshot).containsExactly(freshTick);
    }

    @Test
    @DisplayName("recordTick does not evict ticks within the window")
    void recordTick_keepsFreshTicksWithinWindow() {
        NormalisedTick tick1 = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", Instant.now().minusMillis(500));
        NormalisedTick tick2 = buildTick(ExchangeId.BINANCE, BTC_USDT, "50010", Instant.now());

        store.recordTick(tick1);
        store.recordTick(tick2);

        List<NormalisedTick> snapshot = store.getSnapshot(ExchangeId.BINANCE, BTC_USDT);
        assertThat(snapshot).hasSize(2).containsExactly(tick1, tick2);
    }

    // ─── Key isolation ────────────────────────────────────────────────────────

    @Test
    @DisplayName("ticks for different exchanges are stored independently")
    void recordTick_isolatesByExchange() {
        NormalisedTick binanceTick = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", Instant.now());
        NormalisedTick bybitTick   = buildTick(ExchangeId.BYBIT,   BTC_USDT, "50050", Instant.now());

        store.recordTick(binanceTick);
        store.recordTick(bybitTick);

        assertThat(store.getSnapshot(ExchangeId.BINANCE, BTC_USDT)).containsExactly(binanceTick);
        assertThat(store.getSnapshot(ExchangeId.BYBIT,   BTC_USDT)).containsExactly(bybitTick);
    }

    @Test
    @DisplayName("ticks for different pairs are stored independently")
    void recordTick_isolatesByPair() {
        NormalisedTick btcTick = buildTick(ExchangeId.BINANCE, BTC_USDT, "50000", Instant.now());
        NormalisedTick ethTick = buildTick(ExchangeId.BINANCE, ETH_USDT, "3000",  Instant.now());

        store.recordTick(btcTick);
        store.recordTick(ethTick);

        assertThat(store.getSnapshot(ExchangeId.BINANCE, BTC_USDT)).containsExactly(btcTick);
        assertThat(store.getSnapshot(ExchangeId.BINANCE, ETH_USDT)).containsExactly(ethTick);
    }

    @Test
    @DisplayName("getSnapshot returns empty list for an unknown exchange-pair key")
    void getSnapshot_returnsEmptyForUnknownKey() {
        List<NormalisedTick> snapshot = store.getSnapshot(ExchangeId.KUCOIN, BTC_USDT);

        assertThat(snapshot).isEmpty();
    }

    // ─── Test data builder ────────────────────────────────────────────────────

    private NormalisedTick buildTick(ExchangeId exchangeId, TradingPair pair,
                                     String bidPrice, Instant exchangeTimestamp) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(pair)
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(bidPrice).add(BigDecimal.ONE))
                .bestBidQuantity(BigDecimal.ONE)
                .bestAskQuantity(BigDecimal.ONE)
                .exchangeTimestamp(exchangeTimestamp)
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }
}
