package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DetectionMetrics}.
 *
 * <p>{@link SimpleMeterRegistry} is used so assertions test actual Micrometer state
 * rather than interaction recordings — wrong metric names or tags cause the meter not
 * to exist, making the failure self-explanatory.
 */
class DetectionMetricsTest {

    private SimpleMeterRegistry registry;
    private DetectionMetrics metrics;

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new DetectionMetrics(registry);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordEvent — counters
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("DETECTED event increments opportunities.events counter with status=detected")
    void recordEvent_detected_incrementsCounter() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.DETECTED, 0L);

        metrics.recordEvent(opp);

        Counter counter = registry.find(DetectionMetrics.METRIC_OPPORTUNITY_EVENTS)
                .tag(DetectionMetrics.TAG_STATUS, "detected")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("CLOSED event increments opportunities.events counter with status=closed")
    void recordEvent_closed_incrementsCounter() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.CLOSED, 250L);

        metrics.recordEvent(opp);

        Counter counter = registry.find(DetectionMetrics.METRIC_OPPORTUNITY_EVENTS)
                .tag(DetectionMetrics.TAG_STATUS, "closed")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("EXPIRED event increments opportunities.events counter with status=expired")
    void recordEvent_expired_incrementsCounter() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.EXPIRED, 61_000L);

        metrics.recordEvent(opp);

        Counter counter = registry.find(DetectionMetrics.METRIC_OPPORTUNITY_EVENTS)
                .tag(DetectionMetrics.TAG_STATUS, "expired")
                .counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordEvent — spread distribution (DETECTED only)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("DETECTED event records netSpreadBps in the spread distribution summary")
    void recordEvent_detected_recordsSpreadDistribution() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.DETECTED, 0L);

        metrics.recordEvent(opp);

        DistributionSummary summary = registry.find(DetectionMetrics.METRIC_NET_SPREAD_BPS).summary();
        assertThat(summary).isNotNull();
        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.totalAmount()).isEqualTo(opp.getNetSpreadBps().doubleValue());
    }

    @Test
    @DisplayName("CLOSED event does NOT record to the spread distribution summary")
    void recordEvent_closed_doesNotRecordSpreadDistribution() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.CLOSED, 250L);

        metrics.recordEvent(opp);

        DistributionSummary summary = registry.find(DetectionMetrics.METRIC_NET_SPREAD_BPS).summary();
        // No spread distribution recorded for closed events
        assertThat(summary).isNull();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordEvent — duration distribution (CLOSED and EXPIRED)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("CLOSED event records totalDurationMs in the duration distribution summary")
    void recordEvent_closed_recordsDurationDistribution() {
        ArbitrageOpportunity opp = opportunity(OpportunityStatus.CLOSED, 250L);

        metrics.recordEvent(opp);

        DistributionSummary summary = registry.find(DetectionMetrics.METRIC_OPPORTUNITY_DURATION_MS)
                .tag(DetectionMetrics.TAG_STATUS, "closed")
                .summary();
        assertThat(summary).isNotNull();
        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.totalAmount()).isEqualTo(250.0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // recordComparisonLatency
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("recordComparisonLatency registers a timer tagged with lowercase exchange name")
    void recordComparisonLatency_registersTimerWithExchangeTag() {
        metrics.recordComparisonLatency(ExchangeId.BINANCE, 500_000L); // 500µs

        Timer timer = registry.find(DetectionMetrics.METRIC_COMPARISON_LATENCY)
                .tag(DetectionMetrics.TAG_EXCHANGE, "binance")
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private ArbitrageOpportunity opportunity(OpportunityStatus status, long totalDurationMs) {
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(BTC_USDT)
                .sellExchange(ExchangeId.BINANCE)
                .buyExchange(ExchangeId.KUCOIN)
                .sellPrice(new BigDecimal("65200.00"))
                .buyPrice(new BigDecimal("65000.00"))
                .sellQuantity(new BigDecimal("1.00000000"))
                .buyQuantity(new BigDecimal("1.00000000"))
                .sellFeeRate(ExchangeId.BINANCE.getDefaultTakerFeeRate())
                .buyFeeRate(ExchangeId.KUCOIN.getDefaultTakerFeeRate())
                .grossSpread(new BigDecimal("200.00"))
                .netSpread(new BigDecimal("69.80"))
                .grossSpreadBps(new BigDecimal("30.76923077"))
                .netSpreadBps(new BigDecimal("10.73846154"))
                .arbitrageableQuantity(new BigDecimal("1.00000000"))
                .theoreticalProfit(new BigDecimal("69.80"))
                .status(status)
                .detectionTimestamp(Instant.now())
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(System.nanoTime())
                .closedNanoTime(0L)
                .peakNetSpread(new BigDecimal("69.80"))
                .averageNetSpread(new BigDecimal("69.80"))
                .totalDurationMs(totalDurationMs)
                .updateCount(1L)
                .build();
    }
}
