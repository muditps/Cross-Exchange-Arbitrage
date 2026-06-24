package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OpportunityTracker}.
 *
 * <p>The clock is injected as an {@link AtomicLong} so tests can set "now" to any
 * deterministic value — critical for expiry tests that check duration calculations.
 *
 * <p>{@link DetectionProperties} is constructed directly (not mocked) so
 * property values are real and no stubbing is needed.
 */
class OpportunityTrackerTest {

    private static final long FIXED_NOW_NS  = 10_000_000_000L; // 10s in nanoseconds
    private static final long MAX_DURATION_MS  = 60_000L;
    private static final long MAX_DURATION_NS  = MAX_DURATION_MS * 1_000_000L;

    private static final TradingPair BTC_USDT  = TradingPair.fromSymbol("BTC-USDT");
    private static final TradingPair ETH_USDT  = TradingPair.fromSymbol("ETH-USDT");

    private AtomicLong clock;
    private OpportunityTracker tracker;

    @BeforeEach
    void setUp() {
        clock = new AtomicLong(FIXED_NOW_NS);
        DetectionProperties properties = new DetectionProperties();
        properties.setMaxOpportunityDurationMs(MAX_DURATION_MS);
        tracker = new OpportunityTracker(properties, clock::get);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // New opportunity: DETECTED event
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("first detection for a direction emits DETECTED event and stores as OPEN")
    void update_newDirection_emitsDetectedEvent_storedAsOpen() {
        ArbitrageOpportunity fresh = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");

        List<ArbitrageOpportunity> events = tracker.update(BTC_USDT, List.of(fresh));

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getStatus()).isEqualTo(OpportunityStatus.DETECTED);
        assertThat(events.get(0).getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(tracker.openCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("first detection initialises updateCount to 1 in the stored OPEN entry")
    void update_newDirection_storedEntryHasUpdateCountOne() {
        ArbitrageOpportunity fresh = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");
        tracker.update(BTC_USDT, List.of(fresh));

        // Second tick with same pair but different direction to observe stored state
        // We can observe it via the update returning the closed entry if we pass empty list
        List<ArbitrageOpportunity> closed = tracker.update(BTC_USDT, List.of());

        assertThat(closed).hasSize(1);
        // updateCount in the closed entry started at 1 (never updated), still 1
        assertThat(closed.get(0).getUpdateCount()).isEqualTo(1L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Known direction: update, no event
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("second tick on same direction updates metrics but emits no event")
    void update_existingDirection_noNewEvent_metricsUpdated() {
        ArbitrageOpportunity first  = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");
        ArbitrageOpportunity second = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "80.00");

        tracker.update(BTC_USDT, List.of(first));
        List<ArbitrageOpportunity> events = tracker.update(BTC_USDT, List.of(second));

        // No new publishable event — update is internal
        assertThat(events).isEmpty();
        assertThat(tracker.openCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("peak net spread is the maximum of all observed net spreads")
    void update_existingDirection_peakTracksHighestSeen() {
        ArbitrageOpportunity low  = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "50.00");
        ArbitrageOpportunity high = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "120.00");
        ArbitrageOpportunity low2 = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "40.00");

        tracker.update(BTC_USDT, List.of(low));
        tracker.update(BTC_USDT, List.of(high));
        tracker.update(BTC_USDT, List.of(low2));

        // Close by passing empty list so we can inspect the closed entry
        List<ArbitrageOpportunity> closed = tracker.update(BTC_USDT, List.of());
        assertThat(closed).hasSize(1);
        assertThat(closed.get(0).getPeakNetSpread()).isEqualByComparingTo(new BigDecimal("120.00"));
    }

    @Test
    @DisplayName("average net spread converges using Welford online algorithm")
    void update_existingDirection_averageSpreadConvergesCorrectly() {
        // Three samples: 60, 80, 100 → mean = 80
        ArbitrageOpportunity s1 = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "60.00");
        ArbitrageOpportunity s2 = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "80.00");
        ArbitrageOpportunity s3 = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "100.00");

        tracker.update(BTC_USDT, List.of(s1));
        tracker.update(BTC_USDT, List.of(s2));
        tracker.update(BTC_USDT, List.of(s3));

        List<ArbitrageOpportunity> closed = tracker.update(BTC_USDT, List.of());
        assertThat(closed).hasSize(1);
        // mean(60, 80, 100) = 80
        assertThat(closed.get(0).getAverageNetSpread()).isEqualByComparingTo(new BigDecimal("80.00"));
    }

    @Test
    @DisplayName("updateCount increments on each subsequent tick")
    void update_existingDirection_updateCountIncrements() {
        ArbitrageOpportunity opp = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");

        tracker.update(BTC_USDT, List.of(opp));
        tracker.update(BTC_USDT, List.of(opp));
        tracker.update(BTC_USDT, List.of(opp));

        // 3 ticks → count = 3 (started at 1, incremented twice)
        List<ArbitrageOpportunity> closed = tracker.update(BTC_USDT, List.of());
        assertThat(closed.get(0).getUpdateCount()).isEqualTo(3L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CLOSED event
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("direction absent from fresh set while OPEN emits CLOSED event and is removed")
    void update_directionDisappears_emitsClosedEvent_removedFromMap() {
        ArbitrageOpportunity opp = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");
        tracker.update(BTC_USDT, List.of(opp));

        // Next tick: same pair but no profitable directions
        List<ArbitrageOpportunity> events = tracker.update(BTC_USDT, List.of());

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getStatus()).isEqualTo(OpportunityStatus.CLOSED);
        assertThat(events.get(0).getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(tracker.openCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("closed opportunity has durationMs computed from nanoTime difference")
    void update_closedOpportunity_durationMsComputedCorrectly() {
        ArbitrageOpportunity opp = opportunityAtNano(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN,
                "69.80", FIXED_NOW_NS);
        tracker.update(BTC_USDT, List.of(opp));

        // Advance clock by 250ms
        clock.set(FIXED_NOW_NS + 250_000_000L);
        List<ArbitrageOpportunity> events = tracker.update(BTC_USDT, List.of());

        assertThat(events.get(0).getTotalDurationMs()).isEqualTo(250L);
        assertThat(events.get(0).getClosedNanoTime()).isEqualTo(FIXED_NOW_NS + 250_000_000L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Pair scoping
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("closure detection is scoped to the current pair — other pairs unaffected")
    void update_differentPairs_trackedIndependently() {
        ArbitrageOpportunity btc = opportunity(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "69.80");
        ArbitrageOpportunity eth = opportunity(ETH_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN, "5.50");

        tracker.update(BTC_USDT, List.of(btc));
        tracker.update(ETH_USDT, List.of(eth));
        assertThat(tracker.openCount()).isEqualTo(2);

        // BTC spread disappears — ETH should remain OPEN
        List<ArbitrageOpportunity> events = tracker.update(BTC_USDT, List.of());
        assertThat(events).hasSize(1);
        assertThat(events.get(0).getTradingPair()).isEqualTo(BTC_USDT);
        assertThat(tracker.openCount()).isEqualTo(1); // ETH still open
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EXPIRED: background expiry sweep
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("OPEN opportunity beyond maxDuration is expired and removed from tracker")
    void expireStaleOpportunities_openBeyondMaxDuration_emitsExpiredEvent() {
        ArbitrageOpportunity opp = opportunityAtNano(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN,
                "69.80", FIXED_NOW_NS);
        tracker.update(BTC_USDT, List.of(opp));
        assertThat(tracker.openCount()).isEqualTo(1);

        // Advance clock past the 60s max duration
        clock.set(FIXED_NOW_NS + MAX_DURATION_NS + 1);

        List<ArbitrageOpportunity> expired = tracker.expireStaleOpportunities();

        assertThat(expired).hasSize(1);
        assertThat(expired.get(0).getStatus()).isEqualTo(OpportunityStatus.EXPIRED);
        assertThat(tracker.openCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("OPEN opportunity exactly at maxDuration is NOT expired — check is strict greater-than")
    void expireStaleOpportunities_openExactlyAtMaxDuration_notExpired() {
        ArbitrageOpportunity opp = opportunityAtNano(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN,
                "69.80", FIXED_NOW_NS);
        tracker.update(BTC_USDT, List.of(opp));

        // Advance clock exactly to the max duration boundary
        clock.set(FIXED_NOW_NS + MAX_DURATION_NS);

        List<ArbitrageOpportunity> expired = tracker.expireStaleOpportunities();

        assertThat(expired).isEmpty();
        assertThat(tracker.openCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("expired opportunity has correct durationMs and closedNanoTime")
    void expireStaleOpportunities_durationComputedCorrectly() {
        long detectedAt = FIXED_NOW_NS;
        ArbitrageOpportunity opp = opportunityAtNano(BTC_USDT, ExchangeId.BINANCE, ExchangeId.KUCOIN,
                "69.80", detectedAt);
        tracker.update(BTC_USDT, List.of(opp));

        long expiredAt = FIXED_NOW_NS + MAX_DURATION_NS + 5_000_000_000L; // 60s + 5s
        clock.set(expiredAt);

        List<ArbitrageOpportunity> expired = tracker.expireStaleOpportunities();

        assertThat(expired.get(0).getTotalDurationMs()).isEqualTo((expiredAt - detectedAt) / 1_000_000L);
        assertThat(expired.get(0).getClosedNanoTime()).isEqualTo(expiredAt);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private ArbitrageOpportunity opportunity(TradingPair pair, ExchangeId sell, ExchangeId buy,
                                              String netSpread) {
        return opportunityAtNano(pair, sell, buy, netSpread, FIXED_NOW_NS);
    }

    /**
     * Builds a minimal DETECTED opportunity with a specific detectedNanoTime.
     * The detectedNanoTime controls duration calculations.
     */
    private ArbitrageOpportunity opportunityAtNano(TradingPair pair, ExchangeId sell, ExchangeId buy,
                                                    String netSpread, long detectedNanoTime) {
        BigDecimal net = new BigDecimal(netSpread);
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(pair)
                .sellExchange(sell)
                .buyExchange(buy)
                .sellPrice(new BigDecimal("65200.00"))
                .buyPrice(new BigDecimal("65000.00"))
                .sellQuantity(new BigDecimal("1.00000000"))
                .buyQuantity(new BigDecimal("1.00000000"))
                .sellFeeRate(sell.getDefaultTakerFeeRate())
                .buyFeeRate(buy.getDefaultTakerFeeRate())
                .grossSpread(new BigDecimal("200.00"))
                .netSpread(net)
                .grossSpreadBps(new BigDecimal("30.76923077"))
                .netSpreadBps(new BigDecimal("10.73846154"))
                .arbitrageableQuantity(new BigDecimal("1.00000000"))
                .theoreticalProfit(net)
                .status(OpportunityStatus.DETECTED)
                .detectionTimestamp(Instant.now())
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(detectedNanoTime)
                .closedNanoTime(0L)
                .peakNetSpread(net)
                .averageNetSpread(net)
                .totalDurationMs(0L)
                .updateCount(0L)
                .build();
    }
}
