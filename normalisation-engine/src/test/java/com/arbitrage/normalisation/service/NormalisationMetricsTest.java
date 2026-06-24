package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NormalisationMetrics}.
 *
 * <p>Uses {@link SimpleMeterRegistry} — an in-process, no-export registry that
 * accumulates values in memory. This lets tests assert on exact counter values
 * without any Prometheus or external system involved. The same technique is used
 * in Micrometer's own test suite and is the recommended approach for metric unit tests.
 */
@DisplayName("NormalisationMetrics")
class NormalisationMetricsTest {

    private MeterRegistry registry;
    private NormalisationMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new NormalisationMetrics(registry);
    }

    // ========================================================================
    // recordNormalisedTick
    // ========================================================================

    @Test
    @DisplayName("recordNormalisedTick registers counter with correct name and exchange tag")
    void recordNormalisedTick_registersCounterWithCorrectNameAndTag() {
        metrics.recordNormalisedTick(ExchangeId.BINANCE);

        Counter counter = registry.find("normalisation.ticks.normalised")
                .tag("exchange", "BINANCE")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordNormalisedTick increments counter on each call")
    void recordNormalisedTick_incrementsOnEachCall() {
        metrics.recordNormalisedTick(ExchangeId.BYBIT);
        metrics.recordNormalisedTick(ExchangeId.BYBIT);
        metrics.recordNormalisedTick(ExchangeId.BYBIT);

        Counter counter = registry.find("normalisation.ticks.normalised")
                .tag("exchange", "BYBIT")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("recordNormalisedTick creates separate counters per exchange")
    void recordNormalisedTick_createsSeparateCountersPerExchange() {
        metrics.recordNormalisedTick(ExchangeId.BINANCE);
        metrics.recordNormalisedTick(ExchangeId.BINANCE);
        metrics.recordNormalisedTick(ExchangeId.KUCOIN);

        Counter binanceCounter = registry.find("normalisation.ticks.normalised")
                .tag("exchange", "BINANCE").counter();
        Counter kucoinCounter = registry.find("normalisation.ticks.normalised")
                .tag("exchange", "KUCOIN").counter();

        assertThat(binanceCounter).isNotNull();
        assertThat(kucoinCounter).isNotNull();
        assertThat(binanceCounter.count()).isEqualTo(2.0);
        assertThat(kucoinCounter.count()).isEqualTo(1.0);
    }

    // ========================================================================
    // recordDroppedTick
    // ========================================================================

    @Test
    @DisplayName("recordDroppedTick registers counter with exchange and reason tags")
    void recordDroppedTick_registersCounterWithExchangeAndReasonTags() {
        metrics.recordDroppedTick(ExchangeId.BINANCE, NormalisationMetrics.DROP_REASON_INVALID_FIELDS);

        Counter counter = registry.find("normalisation.ticks.dropped")
                .tag("exchange", "BINANCE")
                .tag("reason", NormalisationMetrics.DROP_REASON_INVALID_FIELDS)
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordDroppedTick creates separate counters per reason tag")
    void recordDroppedTick_createsSeparateCountersPerReason() {
        metrics.recordDroppedTick(ExchangeId.BYBIT, NormalisationMetrics.DROP_REASON_INVALID_FIELDS);
        metrics.recordDroppedTick(ExchangeId.BYBIT, NormalisationMetrics.DROP_REASON_INVALID_FIELDS);
        metrics.recordDroppedTick(ExchangeId.BYBIT, NormalisationMetrics.DROP_REASON_NO_TRANSFORMER);

        Counter invalidCounter = registry.find("normalisation.ticks.dropped")
                .tag("exchange", "BYBIT")
                .tag("reason", NormalisationMetrics.DROP_REASON_INVALID_FIELDS)
                .counter();
        Counter noTransformerCounter = registry.find("normalisation.ticks.dropped")
                .tag("exchange", "BYBIT")
                .tag("reason", NormalisationMetrics.DROP_REASON_NO_TRANSFORMER)
                .counter();

        assertThat(invalidCounter).isNotNull();
        assertThat(noTransformerCounter).isNotNull();
        assertThat(invalidCounter.count()).isEqualTo(2.0);
        assertThat(noTransformerCounter.count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("recordDroppedTick with null exchangeId uses UNKNOWN tag instead of throwing")
    void recordDroppedTick_withNullExchangeId_usesUnknownTag() {
        metrics.recordDroppedTick(null, NormalisationMetrics.DROP_REASON_NULL_TICK);

        Counter counter = registry.find("normalisation.ticks.dropped")
                .tag("exchange", "UNKNOWN")
                .tag("reason", NormalisationMetrics.DROP_REASON_NULL_TICK)
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    // ========================================================================
    // recordProcessingDuration
    // ========================================================================

    @Test
    @DisplayName("recordProcessingDuration registers timer with correct name and exchange tag")
    void recordProcessingDuration_registersTimerWithCorrectNameAndTag() {
        metrics.recordProcessingDuration(ExchangeId.BINANCE, 45_000L); // 45µs

        Timer timer = registry.find("normalisation.processing.duration")
                .tag("exchange", "BINANCE")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(1L);
    }

    @Test
    @DisplayName("recordProcessingDuration records duration correctly in nanoseconds")
    void recordProcessingDuration_recordsDurationInNanoseconds() {
        final long durationNanos = 50_000L; // 50µs

        metrics.recordProcessingDuration(ExchangeId.KUCOIN, durationNanos);

        Timer timer = registry.find("normalisation.processing.duration")
                .tag("exchange", "KUCOIN")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isEqualTo((double) durationNanos);
    }

    @Test
    @DisplayName("recordProcessingDuration accumulates multiple recordings")
    void recordProcessingDuration_accumulatesMultipleRecordings() {
        metrics.recordProcessingDuration(ExchangeId.BYBIT, 30_000L);
        metrics.recordProcessingDuration(ExchangeId.BYBIT, 40_000L);
        metrics.recordProcessingDuration(ExchangeId.BYBIT, 50_000L);

        Timer timer = registry.find("normalisation.processing.duration")
                .tag("exchange", "BYBIT")
                .timer();

        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(3L);
        assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isEqualTo(120_000.0);
    }
}
