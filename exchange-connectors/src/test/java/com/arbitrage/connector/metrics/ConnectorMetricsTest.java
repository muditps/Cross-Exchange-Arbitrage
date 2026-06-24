package com.arbitrage.connector.metrics;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ConnectorMetrics}.
 *
 * <p>Uses Micrometer's {@link SimpleMeterRegistry} — an in-memory registry designed
 * for testing. No Prometheus, no HTTP endpoint, no Docker. Counters, timers, and gauges
 * are queried directly from the registry to verify correct registration, tagging,
 * and value tracking.</p>
 *
 * <p><b>KEY CONCEPT — SimpleMeterRegistry:</b> In production, Spring Boot auto-configures
 * a Prometheus registry that exports metrics via {@code /actuator/prometheus}. In tests,
 * we replace it with SimpleMeterRegistry which stores everything in memory. This is the
 * standard Micrometer testing pattern — the metrics code doesn't know or care which
 * registry implementation it's using.</p>
 */
class ConnectorMetricsTest {

    private SimpleMeterRegistry registry;
    private ConnectorMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new ConnectorMetrics(registry);
    }

    // ========================================================================
    // RATE metrics — message throughput
    // ========================================================================

    @Nested
    @DisplayName("Rate Metrics (messages received/parsed)")
    class RateMetrics {

        @Test
        @DisplayName("recordMessageReceived increments counter with exchange tag")
        void recordMessageReceived_incrementsCounter() {
            metrics.recordMessageReceived(ExchangeId.BINANCE);
            metrics.recordMessageReceived(ExchangeId.BINANCE);
            metrics.recordMessageReceived(ExchangeId.BINANCE);

            Counter counter = registry.find("exchange.messages.received")
                    .tag("exchange", "BINANCE")
                    .counter();

            assertNotNull(counter, "Counter should be registered");
            assertEquals(3.0, counter.count(), "Counter should be 3 after 3 increments");
        }

        @Test
        @DisplayName("recordMessageParsed increments counter with exchange tag")
        void recordMessageParsed_incrementsCounter() {
            metrics.recordMessageParsed(ExchangeId.BINANCE);
            metrics.recordMessageParsed(ExchangeId.BINANCE);

            Counter counter = registry.find("exchange.messages.parsed")
                    .tag("exchange", "BINANCE")
                    .counter();

            assertNotNull(counter, "Counter should be registered");
            assertEquals(2.0, counter.count(), "Counter should be 2 after 2 increments");
        }

        @Test
        @DisplayName("Counters for different exchanges are independent")
        void counters_forDifferentExchanges_areIndependent() {
            metrics.recordMessageReceived(ExchangeId.BINANCE);
            metrics.recordMessageReceived(ExchangeId.BINANCE);
            metrics.recordMessageReceived(ExchangeId.BYBIT);

            Counter binanceCounter = registry.find("exchange.messages.received")
                    .tag("exchange", "BINANCE")
                    .counter();
            Counter bybitCounter = registry.find("exchange.messages.received")
                    .tag("exchange", "BYBIT")
                    .counter();

            assertNotNull(binanceCounter);
            assertNotNull(bybitCounter);
            assertEquals(2.0, binanceCounter.count(), "BINANCE should have 2");
            assertEquals(1.0, bybitCounter.count(), "BYBIT should have 1");
        }
    }

    // ========================================================================
    // ERROR metrics — parse failures and reconnections
    // ========================================================================

    @Nested
    @DisplayName("Error Metrics (skips, parse errors, reconnections)")
    class ErrorMetrics {

        @Test
        @DisplayName("recordMessageSkipped increments counter with exchange and reason tags")
        void recordMessageSkipped_incrementsWithReasonTag() {
            metrics.recordMessageSkipped(ExchangeId.BINANCE, "non_book_ticker");
            metrics.recordMessageSkipped(ExchangeId.BINANCE, "non_book_ticker");
            metrics.recordMessageSkipped(ExchangeId.BINANCE, "invalid_fields");

            Counter nonBookTicker = registry.find("exchange.messages.skipped")
                    .tag("exchange", "BINANCE")
                    .tag("reason", "non_book_ticker")
                    .counter();
            Counter invalidFields = registry.find("exchange.messages.skipped")
                    .tag("exchange", "BINANCE")
                    .tag("reason", "invalid_fields")
                    .counter();

            assertNotNull(nonBookTicker);
            assertNotNull(invalidFields);
            assertEquals(2.0, nonBookTicker.count(), "non_book_ticker should be 2");
            assertEquals(1.0, invalidFields.count(), "invalid_fields should be 1");
        }

        @Test
        @DisplayName("recordParseError increments counter with exchange and type tags")
        void recordParseError_incrementsWithTypeTag() {
            metrics.recordParseError(ExchangeId.BINANCE, "json_parse");
            metrics.recordParseError(ExchangeId.BINANCE, "number_format");
            metrics.recordParseError(ExchangeId.BINANCE, "json_parse");

            Counter jsonParse = registry.find("exchange.messages.errors")
                    .tag("exchange", "BINANCE")
                    .tag("type", "json_parse")
                    .counter();
            Counter numberFormat = registry.find("exchange.messages.errors")
                    .tag("exchange", "BINANCE")
                    .tag("type", "number_format")
                    .counter();

            assertNotNull(jsonParse);
            assertNotNull(numberFormat);
            assertEquals(2.0, jsonParse.count(), "json_parse should be 2");
            assertEquals(1.0, numberFormat.count(), "number_format should be 1");
        }

        @Test
        @DisplayName("recordReconnection increments counter with exchange tag")
        void recordReconnection_incrementsCounter() {
            metrics.recordReconnection(ExchangeId.BINANCE);

            Counter counter = registry.find("exchange.reconnections")
                    .tag("exchange", "BINANCE")
                    .counter();

            assertNotNull(counter);
            assertEquals(1.0, counter.count());
        }
    }

    // ========================================================================
    // DURATION metrics — parse latency
    // ========================================================================

    @Nested
    @DisplayName("Duration Metrics (parse latency)")
    class DurationMetrics {

        @Test
        @DisplayName("recordParseDuration registers timer with exchange tag")
        void recordParseDuration_registersTimer() {
            long durationNanos = 15_000; // 15µs

            metrics.recordParseDuration(ExchangeId.BINANCE, durationNanos);

            Timer timer = registry.find("exchange.parse.duration")
                    .tag("exchange", "BINANCE")
                    .timer();

            assertNotNull(timer, "Timer should be registered");
            assertEquals(1, timer.count(), "Timer should have 1 recording");
            assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) > 0,
                    "Total time should be positive");
        }

        @Test
        @DisplayName("Multiple recordings accumulate in the timer")
        void multipleParseDurations_accumulate() {
            metrics.recordParseDuration(ExchangeId.BINANCE, 10_000); // 10µs
            metrics.recordParseDuration(ExchangeId.BINANCE, 20_000); // 20µs
            metrics.recordParseDuration(ExchangeId.BINANCE, 30_000); // 30µs

            Timer timer = registry.find("exchange.parse.duration")
                    .tag("exchange", "BINANCE")
                    .timer();

            assertNotNull(timer);
            assertEquals(3, timer.count(), "Timer should have 3 recordings");
        }
    }

    // ========================================================================
    // GAUGE metrics — feed status
    // ========================================================================

    @Nested
    @DisplayName("Gauge Metrics (feed status)")
    class GaugeMetrics {

        @Test
        @DisplayName("recordFeedStatus registers gauge with correct ordinal value")
        void recordFeedStatus_registersGaugeWithOrdinal() {
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.CONNECTED);

            Gauge gauge = registry.find("exchange.feed.status")
                    .tag("exchange", "BINANCE")
                    .gauge();

            assertNotNull(gauge, "Gauge should be registered");
            assertEquals(FeedStatus.CONNECTED.ordinal(), (int) gauge.value(),
                    "Gauge value should be CONNECTED ordinal");
        }

        @Test
        @DisplayName("Feed status gauge updates on state transitions")
        void feedStatusGauge_updatesOnTransition() {
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.DISCONNECTED);
            Gauge gauge = registry.find("exchange.feed.status")
                    .tag("exchange", "BINANCE")
                    .gauge();
            assertNotNull(gauge);
            assertEquals(FeedStatus.DISCONNECTED.ordinal(), (int) gauge.value());

            // Transition to CONNECTED
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.CONNECTED);
            assertEquals(FeedStatus.CONNECTED.ordinal(), (int) gauge.value(),
                    "Gauge should reflect CONNECTED after transition");

            // Transition to STALE
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.STALE);
            assertEquals(FeedStatus.STALE.ordinal(), (int) gauge.value(),
                    "Gauge should reflect STALE after transition");

            // Transition to RECONNECTING
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.RECONNECTING);
            assertEquals(FeedStatus.RECONNECTING.ordinal(), (int) gauge.value(),
                    "Gauge should reflect RECONNECTING after transition");
        }

        @Test
        @DisplayName("Feed status gauges for different exchanges are independent")
        void feedStatusGauges_forDifferentExchanges_areIndependent() {
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.CONNECTED);
            metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.DISCONNECTED);

            Gauge binanceGauge = registry.find("exchange.feed.status")
                    .tag("exchange", "BINANCE")
                    .gauge();
            Gauge bybitGauge = registry.find("exchange.feed.status")
                    .tag("exchange", "BYBIT")
                    .gauge();

            assertNotNull(binanceGauge);
            assertNotNull(bybitGauge);
            assertEquals(FeedStatus.CONNECTED.ordinal(), (int) binanceGauge.value());
            assertEquals(FeedStatus.DISCONNECTED.ordinal(), (int) bybitGauge.value());
        }
    }
}
