package com.arbitrage.detection.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.model.PriceState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StalenessFilter}.
 *
 * <p>The clock is injected as an {@link AtomicLong} so tests can set "now" to any value
 * and compute exact expected ages. No Spring context needed — all dependencies constructed
 * manually.
 *
 * <p>{@link SimpleMeterRegistry} is used instead of a mock so counter assertions verify
 * actual Micrometer state rather than interaction recording.
 */
class StalenessFilterTest {

    private static final long THRESHOLD_MS  = 500L;
    private static final long THRESHOLD_NS  = THRESHOLD_MS * 1_000_000L;
    private static final long FIXED_NOW_NS  = 2_000_000_000L; // arbitrary fixed "now" in nanoseconds

    private AtomicLong clock;
    private SimpleMeterRegistry meterRegistry;
    private StalenessFilter filter;

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");

    @BeforeEach
    void setUp() {
        clock = new AtomicLong(FIXED_NOW_NS);
        meterRegistry = new SimpleMeterRegistry();

        DetectionProperties properties = new DetectionProperties();
        properties.setStalenessThresholdMs(THRESHOLD_MS);

        filter = new StalenessFilter(properties, clock::get, meterRegistry);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // isStale — age boundaries
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("price received at exactly 'now' (age = 0) is not stale")
    void isStale_ageZero_returnsFalse() {
        PriceState state = priceStateWithReceivedAt(FIXED_NOW_NS);

        assertThat(filter.isStale(state)).isFalse();
    }

    @Test
    @DisplayName("price age below threshold is not stale")
    void isStale_ageBelowThreshold_returnsFalse() {
        // 499ms old — just under the 500ms threshold
        long receivedNs = FIXED_NOW_NS - (499 * 1_000_000L);
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isFalse();
    }

    @Test
    @DisplayName("price age exactly at threshold is NOT stale — check is strict greater-than")
    void isStale_ageExactlyAtThreshold_returnsFalse() {
        // Exactly 500ms old — the check is `age > threshold`, so this is still fresh
        long receivedNs = FIXED_NOW_NS - THRESHOLD_NS;
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isFalse();
    }

    @Test
    @DisplayName("price age exceeding threshold by 1 nanosecond is stale")
    void isStale_ageJustAboveThreshold_returnsTrue() {
        // 500ms + 1ns — just over the threshold
        long receivedNs = FIXED_NOW_NS - THRESHOLD_NS - 1;
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isTrue();
    }

    @Test
    @DisplayName("price significantly older than threshold is stale")
    void isStale_ageMuchAboveThreshold_returnsTrue() {
        // 2 seconds old — well beyond 500ms threshold
        long receivedNs = FIXED_NOW_NS - 2_000_000_000L;
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isTrue();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // isStale — threshold unit conversion (ms → ns)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * KEY CONCEPT: {@code receivedTimestamp} is in nanoseconds ({@code System.nanoTime()}).
     * The configured {@code stalenessThresholdMs} is in milliseconds. The filter must
     * convert: {@code thresholdNs = thresholdMs × 1,000,000}. Getting this wrong causes
     * a 1000x error — a 500ms threshold treated as 500µs would mark every price as stale;
     * treated as 500s would never mark any price as stale.
     */
    @Test
    @DisplayName("threshold ms→ns conversion: 500ms threshold rejects 501ms old price")
    void isStale_thresholdMsToNsConversion_correct() {
        // 501ms old in nanoseconds
        long receivedNs = FIXED_NOW_NS - (501L * 1_000_000L);
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isTrue();

        // 499ms old — should NOT be stale with 500ms threshold
        long freshReceivedNs = FIXED_NOW_NS - (499L * 1_000_000L);
        PriceState freshState = priceStateWithReceivedAt(freshReceivedNs);
        assertThat(filter.isStale(freshState)).isFalse();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordStaleSkip — metrics
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("recordStaleSkip increments detection.stale.skips counter")
    void recordStaleSkip_incrementsStaleSkipsCounter() {
        PriceState stale = priceStateWithReceivedAt(FIXED_NOW_NS - THRESHOLD_NS - 1);

        filter.recordStaleSkip(ExchangeId.BINANCE, BTC_USDT, stale);

        Counter counter = meterRegistry.find(StalenessFilter.METRIC_STALE_SKIPS)
                .tag(StalenessFilter.TAG_EXCHANGE, "binance")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordStaleSkip tags counter with lowercase exchange name")
    void recordStaleSkip_counterTaggedWithLowercaseExchangeName() {
        PriceState stale = priceStateWithReceivedAt(FIXED_NOW_NS - THRESHOLD_NS - 1);

        filter.recordStaleSkip(ExchangeId.KUCOIN, BTC_USDT, stale);

        // Tag value should be lowercase exchange name (kucoin not KUCOIN)
        Counter counter = meterRegistry.find(StalenessFilter.METRIC_STALE_SKIPS)
                .tag(StalenessFilter.TAG_EXCHANGE, "kucoin")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordStaleSkip for different exchanges increments separate counters")
    void recordStaleSkip_differentExchanges_separateCounters() {
        PriceState stale = priceStateWithReceivedAt(FIXED_NOW_NS - THRESHOLD_NS - 1);

        filter.recordStaleSkip(ExchangeId.BINANCE, BTC_USDT, stale);
        filter.recordStaleSkip(ExchangeId.BINANCE, BTC_USDT, stale);
        filter.recordStaleSkip(ExchangeId.BYBIT,   BTC_USDT, stale);

        assertThat(meterRegistry.find(StalenessFilter.METRIC_STALE_SKIPS)
                .tag(StalenessFilter.TAG_EXCHANGE, "binance")
                .counter().count()).isEqualTo(2.0);

        assertThat(meterRegistry.find(StalenessFilter.METRIC_STALE_SKIPS)
                .tag(StalenessFilter.TAG_EXCHANGE, "bybit")
                .counter().count()).isEqualTo(1.0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Clock advancement
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("advancing the clock past the threshold makes a previously-fresh price stale")
    void isStale_clockAdvancesMakesFreshPriceStale() {
        // Price received at fixedNow — fresh at t=0
        long receivedNs = FIXED_NOW_NS;
        PriceState state = priceStateWithReceivedAt(receivedNs);

        assertThat(filter.isStale(state)).isFalse();

        // Advance clock by 501ms — now the same price is stale
        clock.set(FIXED_NOW_NS + (501L * 1_000_000L));

        assertThat(filter.isStale(state)).isTrue();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private PriceState priceStateWithReceivedAt(long receivedTimestampNanos) {
        return PriceState.builder()
                .bestBidPrice(new BigDecimal("65000.00"))
                .bestAskPrice(new BigDecimal("65001.00"))
                .bestBidQuantity(new BigDecimal("1.00000000"))
                .bestAskQuantity(new BigDecimal("1.00000000"))
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(receivedTimestampNanos)
                .processedTimestamp(receivedTimestampNanos + 100_000L)
                .build();
    }
}
