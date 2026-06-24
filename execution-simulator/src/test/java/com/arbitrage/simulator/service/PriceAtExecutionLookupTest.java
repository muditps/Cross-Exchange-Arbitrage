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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PriceAtExecutionLookup}.
 *
 * <p>Tests verify the floor-lookup semantics: given a target {@link Instant},
 * the lookup returns the most recent tick whose {@code exchangeTimestamp} is
 * at or before the target. Uses a real {@link HistoricalPriceStore} (not mocked)
 * to test the two components together as intended.
 */
class PriceAtExecutionLookupTest {

    private static final TradingPair BTC_USDT  = TradingPair.fromSymbol("BTC-USDT");
    private static final ExchangeId  EXCHANGE   = ExchangeId.BINANCE;

    // Base time relative to now so ticks always fall within the 300-second eviction window.
    // A fixed past date (e.g. 2024-01-01) would be immediately evicted by the store
    // because eviction threshold = Instant.now().minusSeconds(300) is still in 2026.
    private static final Instant BASE = Instant.now();

    private HistoricalPriceStore  store;
    private PriceAtExecutionLookup lookup;

    @BeforeEach
    void setUp() {
        SimulationProperties props = new SimulationProperties();
        props.setHistoricalWindowSeconds(300); // 5-minute window: no eviction during tests
        store  = new HistoricalPriceStore(props);
        lookup = new PriceAtExecutionLookup(store);
    }

    // ─── Empty store ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("returns empty when the store has no ticks for the given key")
    void findClosestBefore_returnsEmptyWhenStoreEmpty() {
        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE);

        assertThat(result).isEmpty();
    }

    // ─── Exact timestamp match ────────────────────────────────────────────────

    @Test
    @DisplayName("returns the tick whose exchangeTimestamp exactly equals targetTime")
    void findClosestBefore_exactTimestampMatch() {
        NormalisedTick tick = buildTick("50000", BASE);
        store.recordTick(tick);

        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE);

        assertThat(result).contains(tick);
    }

    // ─── Floor lookup ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("returns the newest tick whose timestamp is strictly before targetTime")
    void findClosestBefore_returnsLastTickBeforeTarget() {
        NormalisedTick olderTick  = buildTick("49900", BASE.minusMillis(100));
        NormalisedTick newerTick  = buildTick("50000", BASE.minusMillis(50));
        // targetTime = BASE.minusMillis(10): both olderTick and newerTick qualify,
        // newerTick is the floor result (most recent before target)
        store.recordTick(olderTick);
        store.recordTick(newerTick);

        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE.minusMillis(10));

        assertThat(result).contains(newerTick);
    }

    @Test
    @DisplayName("returns the single tick at or before targetTime when only one qualifies")
    void findClosestBefore_returnsSingleQualifyingTick() {
        NormalisedTick beforeTick = buildTick("50000", BASE.minusMillis(30));
        NormalisedTick afterTick  = buildTick("50050", BASE.plusMillis(30));
        store.recordTick(beforeTick);
        store.recordTick(afterTick);

        // targetTime = BASE: beforeTick qualifies, afterTick does not
        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE);

        assertThat(result).contains(beforeTick);
    }

    // ─── No qualifying tick ───────────────────────────────────────────────────

    @Test
    @DisplayName("returns empty when all stored ticks have timestamps after targetTime")
    void findClosestBefore_returnsEmptyWhenAllTicksAfterTarget() {
        store.recordTick(buildTick("50000", BASE.plusMillis(10)));
        store.recordTick(buildTick("50010", BASE.plusMillis(20)));

        // targetTime = BASE: all ticks are after BASE, none qualify
        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE);

        assertThat(result).isEmpty();
    }

    // ─── Target after all ticks ───────────────────────────────────────────────

    @Test
    @DisplayName("returns the newest stored tick when targetTime is after all ticks")
    void findClosestBefore_returnsNewestTickWhenTargetIsAfterAll() {
        NormalisedTick tick1 = buildTick("50000", BASE.minusMillis(200));
        NormalisedTick tick2 = buildTick("50010", BASE.minusMillis(100));
        NormalisedTick tick3 = buildTick("50020", BASE.minusMillis(50));
        store.recordTick(tick1);
        store.recordTick(tick2);
        store.recordTick(tick3);

        // targetTime = BASE: all ticks are before BASE, returns newest (tick3)
        Optional<NormalisedTick> result = lookup.findClosestBefore(EXCHANGE, BTC_USDT, BASE);

        assertThat(result).contains(tick3);
    }

    // ─── Test data builder ────────────────────────────────────────────────────

    private NormalisedTick buildTick(String bidPrice, Instant exchangeTimestamp) {
        return NormalisedTick.builder()
                .exchangeId(EXCHANGE)
                .tradingPair(BTC_USDT)
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
