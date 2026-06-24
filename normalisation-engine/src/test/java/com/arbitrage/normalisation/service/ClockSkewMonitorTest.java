package com.arbitrage.normalisation.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.arbitrage.common.model.ExchangeId;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ClockSkewMonitor}.
 *
 * <p>Wall-clock time is injected via {@code AtomicLong} — tests control "now" precisely
 * without system clock dependency. WARN log detection uses a Logback {@link ListAppender}
 * attached directly to the {@code ClockSkewMonitor} logger — this is the standard in-process
 * log capture approach for Spring Boot projects (no extra dependencies required).
 */
@DisplayName("ClockSkewMonitor")
class ClockSkewMonitorTest {

    private static final long JUMP_THRESHOLD_MS = 500L;
    private static final double EWMA_ALPHA = 0.1;

    private AtomicLong fakeWallClockMs;
    private MeterRegistry registry;
    private ClockSkewMonitor monitor;

    // Logback appender for WARN log assertions
    private ListAppender<ILoggingEvent> logAppender;
    private Logger clockSkewLogger;

    @BeforeEach
    void setUp() {
        fakeWallClockMs = new AtomicLong(1_000_000L); // arbitrary epoch ms base
        registry = new SimpleMeterRegistry();
        monitor = new ClockSkewMonitor(fakeWallClockMs::get, JUMP_THRESHOLD_MS, EWMA_ALPHA, registry);

        // Attach a ListAppender to capture log events from ClockSkewMonitor
        clockSkewLogger = (Logger) LoggerFactory.getLogger(ClockSkewMonitor.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        clockSkewLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        clockSkewLogger.detachAppender(logAppender);
    }

    // ========================================================================
    // First observation
    // ========================================================================

    @Nested
    @DisplayName("first observation — EWMA seeded with initial offset")
    class FirstObservation {

        @Test
        @DisplayName("first tick seeds rolling mean with exact current offset")
        void firstTickSeedsRollingMeanWithCurrentOffset() {
            // localMs=1_000_000, exchangeMs=999_950 → offset = +50ms
            Instant exchangeTimestamp = Instant.ofEpochMilli(999_950L);

            monitor.recordTick(ExchangeId.BINANCE, exchangeTimestamp);

            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BINANCE)).isEqualTo(50L);
        }

        @Test
        @DisplayName("first tick does not trigger WARN (no previous mean to compare against)")
        void firstTickDoesNotTriggerWarn() {
            Instant exchangeTimestamp = Instant.ofEpochMilli(0L); // huge offset but first tick

            monitor.recordTick(ExchangeId.BYBIT, exchangeTimestamp);

            boolean hasWarn = logAppender.list.stream()
                    .anyMatch(e -> e.getLevel() == Level.WARN);
            assertThat(hasWarn).isFalse();
        }

        @Test
        @DisplayName("unseen exchange returns 0 from getRollingMeanOffsetMs")
        void unseenExchangeReturnsZero() {
            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.KUCOIN)).isEqualTo(0L);
        }

        @Test
        @DisplayName("null exchangeTimestamp is silently ignored — no exception, no state change")
        void nullExchangeTimestampIsIgnored() {
            monitor.recordTick(ExchangeId.BINANCE, null);

            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BINANCE)).isEqualTo(0L);
        }
    }

    // ========================================================================
    // EWMA updates
    // ========================================================================

    @Nested
    @DisplayName("EWMA updates — rolling mean blends new observations")
    class EwmaUpdates {

        @Test
        @DisplayName("second tick applies alpha blending: newMean = alpha*current + (1-alpha)*previous")
        void secondTickAppliesAlphaBlending() {
            // Seed: offset = 100ms
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 100));
            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BINANCE)).isEqualTo(100L);

            // Second tick: offset = 200ms
            // EWMA = 0.1 * 200 + 0.9 * 100 = 20 + 90 = 110ms
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 200));

            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BINANCE)).isEqualTo(110L);
        }

        @Test
        @DisplayName("stable offset converges EWMA toward the stable value over many ticks")
        void stableOffsetConvergesEwma() {
            // Seed with 0ms offset
            monitor.recordTick(ExchangeId.BYBIT, Instant.ofEpochMilli(fakeWallClockMs.get()));

            // 100 ticks all at 200ms offset — EWMA should approach 200ms
            for (int i = 0; i < 100; i++) {
                monitor.recordTick(ExchangeId.BYBIT, Instant.ofEpochMilli(fakeWallClockMs.get() - 200));
            }

            // After 100 ticks at alpha=0.1, EWMA ≈ 200ms * (1 - 0.9^100) ≈ 199.997ms
            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BYBIT)).isGreaterThan(195L);
            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BYBIT)).isLessThanOrEqualTo(200L);
        }

        @Test
        @DisplayName("each exchange maintains an independent EWMA")
        void eachExchangeHasIndependentEwma() {
            // BINANCE: offset 50ms
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));

            // KUCOIN: offset 300ms
            monitor.recordTick(ExchangeId.KUCOIN, Instant.ofEpochMilli(fakeWallClockMs.get() - 300));

            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BINANCE)).isEqualTo(50L);
            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.KUCOIN)).isEqualTo(300L);
        }

        @Test
        @DisplayName("negative offset (exchange clock ahead of local) is handled correctly")
        void negativeOffsetHandledCorrectly() {
            // Exchange timestamp is 100ms in the future relative to local — offset = -100ms
            monitor.recordTick(ExchangeId.BYBIT, Instant.ofEpochMilli(fakeWallClockMs.get() + 100));

            assertThat(monitor.getRollingMeanOffsetMs(ExchangeId.BYBIT)).isEqualTo(-100L);
        }
    }

    // ========================================================================
    // Jump detection
    // ========================================================================

    @Nested
    @DisplayName("jump detection — WARN on deviation exceeding threshold")
    class JumpDetection {

        @Test
        @DisplayName("deviation above threshold triggers WARN log")
        void deviationAboveThresholdTriggersWarn() {
            // Seed EWMA with 50ms offset
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));

            // New tick: offset = 700ms — deviation = 650ms > 500ms threshold
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 700));

            boolean hasSkewWarn = logAppender.list.stream()
                    .filter(e -> e.getLevel() == Level.WARN)
                    .anyMatch(e -> e.getFormattedMessage().contains("Clock skew jump detected"));
            assertThat(hasSkewWarn).isTrue();
        }

        @Test
        @DisplayName("deviation exactly at threshold does NOT trigger WARN (threshold is exclusive)")
        void deviationAtThresholdDoesNotTriggerWarn() {
            // Seed EWMA with 50ms offset
            monitor.recordTick(ExchangeId.KUCOIN, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));

            // New tick: offset = 550ms — deviation = exactly 500ms = threshold (not above)
            monitor.recordTick(ExchangeId.KUCOIN, Instant.ofEpochMilli(fakeWallClockMs.get() - 550));

            boolean hasWarn = logAppender.list.stream()
                    .anyMatch(e -> e.getLevel() == Level.WARN);
            assertThat(hasWarn).isFalse();
        }

        @Test
        @DisplayName("deviation below threshold does not trigger WARN")
        void deviationBelowThresholdNoWarn() {
            // Seed EWMA with 50ms offset
            monitor.recordTick(ExchangeId.BYBIT, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));

            // New tick: offset = 300ms — deviation = 250ms < 500ms threshold
            monitor.recordTick(ExchangeId.BYBIT, Instant.ofEpochMilli(fakeWallClockMs.get() - 300));

            boolean hasWarn = logAppender.list.stream()
                    .anyMatch(e -> e.getLevel() == Level.WARN);
            assertThat(hasWarn).isFalse();
        }

        @Test
        @DisplayName("WARN includes exchange, current offset, rolling mean, and deviation in message")
        void warnMessageContainsRequiredContext() {
            // Seed with 50ms offset
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));

            // Trigger jump: offset = 700ms
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 700));

            String warnMessage = logAppender.list.stream()
                    .filter(e -> e.getLevel() == Level.WARN)
                    .map(ILoggingEvent::getFormattedMessage)
                    .findFirst()
                    .orElse("");

            assertThat(warnMessage).contains("BINANCE");
            assertThat(warnMessage).contains("700");   // current offset
            assertThat(warnMessage).contains("50");    // rolling mean
            assertThat(warnMessage).contains("650");   // deviation
        }
    }

    // ========================================================================
    // Micrometer gauge
    // ========================================================================

    @Nested
    @DisplayName("Micrometer gauge — normalisation.clock.skew.offset")
    class MicrometerGauge {

        @Test
        @DisplayName("gauge is registered on first tick with correct exchange tag")
        void gaugeRegisteredOnFirstTick() {
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 75));

            Gauge gauge = registry.find("normalisation.clock.skew.offset")
                    .tag("exchange", "BINANCE")
                    .gauge();

            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(75.0);
        }

        @Test
        @DisplayName("gauge value updates when rolling mean changes")
        void gaugeValueUpdatesWithRollingMean() {
            // Seed: 100ms
            monitor.recordTick(ExchangeId.KUCOIN, Instant.ofEpochMilli(fakeWallClockMs.get() - 100));

            // Second tick: 200ms → EWMA = 0.1*200 + 0.9*100 = 110ms
            monitor.recordTick(ExchangeId.KUCOIN, Instant.ofEpochMilli(fakeWallClockMs.get() - 200));

            Gauge gauge = registry.find("normalisation.clock.skew.offset")
                    .tag("exchange", "KUCOIN")
                    .gauge();

            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(110.0);
        }

        @Test
        @DisplayName("separate gauges registered per exchange")
        void separateGaugesPerExchange() {
            monitor.recordTick(ExchangeId.BINANCE, Instant.ofEpochMilli(fakeWallClockMs.get() - 50));
            monitor.recordTick(ExchangeId.BYBIT,   Instant.ofEpochMilli(fakeWallClockMs.get() - 150));

            Gauge binanceGauge = registry.find("normalisation.clock.skew.offset").tag("exchange", "BINANCE").gauge();
            Gauge bybitGauge   = registry.find("normalisation.clock.skew.offset").tag("exchange", "BYBIT").gauge();

            assertThat(binanceGauge).isNotNull();
            assertThat(bybitGauge).isNotNull();
            assertThat(binanceGauge.value()).isEqualTo(50.0);
            assertThat(bybitGauge.value()).isEqualTo(150.0);
        }
    }
}
