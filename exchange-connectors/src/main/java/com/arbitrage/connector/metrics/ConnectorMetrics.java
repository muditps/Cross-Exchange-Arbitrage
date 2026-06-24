package com.arbitrage.connector.metrics;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer-based metrics for exchange connectors, following the RED pattern:
 * Rate (messages/sec), Errors (parse failures, disconnects), Duration (parse latency).
 *
 * <p><b>Why RED?</b> These three categories cover most observability needs for any
 * service. If you can answer "how many?" (rate), "how often do they fail?" (errors),
 * and "how long do they take?" (duration), you understand your system. This is the
 * same monitoring pattern used at Google (Golden Signals) and trading firms for
 * real-time feed health monitoring.</p>
 *
 * <p><b>Exchange-agnostic design:</b> All metrics are tagged with {@code exchange}
 * (e.g., "BINANCE", "BYBIT"). When adding a new exchange connector, no changes to
 * this class are needed — just call the same methods with a different ExchangeId.
 * Prometheus queries like {@code rate(exchange_messages_received_total{exchange="BINANCE"}[1m])}
 * filter by tag.</p>
 *
 * <p><b>Thread safety:</b> All Micrometer counters and timers are thread-safe by design.
 * The feed status gauges use {@link AtomicInteger} for lock-free concurrent access.
 * Multiple connector threads can call these methods concurrently.</p>
 *
 * <p><b>Metric naming convention:</b> Micrometer uses dot-separated names
 * ({@code exchange.messages.received}) which are automatically converted to the
 * backend's convention — Prometheus converts dots to underscores and appends
 * {@code _total} for counters: {@code exchange_messages_received_total}.</p>
 *
 * @see <a href="https://grafana.com/blog/2018/08/02/the-red-method-how-to-instrument-your-services/">
 *     The RED Method</a>
 */
@Component
@Slf4j
public class ConnectorMetrics {

    private static final String TAG_EXCHANGE = "exchange";
    private static final String TAG_REASON = "reason";
    private static final String TAG_ERROR_TYPE = "type";

    private final MeterRegistry registry;

    /**
     * Stores the current feed status ordinal per exchange for gauge reporting.
     *
     * <p>Gauges in Micrometer observe a value supplier — they don't have an increment/set
     * method. We store the value in an AtomicInteger and point the gauge at it. When
     * Prometheus scrapes, the gauge reads the current value from the AtomicInteger.</p>
     *
     * <p>Map is lazily populated on first call to {@link #recordFeedStatus} per exchange.
     * ConcurrentHashMap because multiple exchanges may register simultaneously.</p>
     */
    private final Map<ExchangeId, AtomicInteger> feedStatusGauges = new ConcurrentHashMap<>();

    /**
     * Creates connector metrics and registers them with the Micrometer registry.
     *
     * <p>Spring Boot auto-configures a {@link MeterRegistry} that exports to Prometheus
     * via the {@code /actuator/prometheus} endpoint. No manual wiring needed.</p>
     *
     * @param registry the Micrometer meter registry (auto-injected by Spring Boot)
     */
    public ConnectorMetrics(MeterRegistry registry) {
        this.registry = registry;
        log.info("ConnectorMetrics initialised with registry: {}", registry.getClass().getSimpleName());
    }

    // ========================================================================
    // RATE — How many messages are flowing through the system?
    // ========================================================================

    /**
     * Records a WebSocket message received from an exchange (before parsing).
     *
     * <p>This counter increments for EVERY message — valid bookTicker, subscription
     * ack, error message, anything. It measures raw throughput from the exchange.
     * Compare with {@link #recordMessageParsed} to see the valid/total ratio.</p>
     *
     * @param exchangeId the exchange that sent the message
     */
    public void recordMessageReceived(ExchangeId exchangeId) {
        Counter.builder("exchange.messages.received")
                .description("Total WebSocket messages received from exchange")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .increment();
    }

    /**
     * Records a message successfully parsed into a {@link com.arbitrage.common.model.NormalisedTick}.
     *
     * <p>Only increments when the parser returns a non-empty Optional — meaning
     * the message was a valid bookTicker with parseable prices. The ratio
     * {@code parsed / received} tells you what percentage of messages are useful data
     * vs protocol overhead (acks, pings, etc.).</p>
     *
     * @param exchangeId the exchange that produced the tick
     */
    public void recordMessageParsed(ExchangeId exchangeId) {
        Counter.builder("exchange.messages.parsed")
                .description("Messages successfully parsed into NormalisedTick")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .increment();
    }

    // ========================================================================
    // ERRORS — What is going wrong?
    // ========================================================================

    /**
     * Records a message that was skipped (returned Optional.empty from the parser).
     *
     * <p>Not all skips are errors — subscription acks and pings are expected.
     * The {@code reason} tag distinguishes expected skips ("non_book_ticker")
     * from problematic ones ("invalid_fields", "validation_failed").</p>
     *
     * @param exchangeId the exchange that sent the message
     * @param reason     why the message was skipped (e.g., "non_book_ticker", "invalid_fields")
     */
    public void recordMessageSkipped(ExchangeId exchangeId, String reason) {
        Counter.builder("exchange.messages.skipped")
                .description("Messages skipped during parsing")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .tag(TAG_REASON, reason)
                .register(registry)
                .increment();
    }

    /**
     * Records a parse error (malformed JSON, invalid number, etc.).
     *
     * <p>These are distinct from skips — errors mean something went wrong with the
     * data, not that the message type was expected to be skipped. A spike in errors
     * could indicate an exchange API change or network corruption.</p>
     *
     * @param exchangeId the exchange that sent the broken message
     * @param errorType  the error category (e.g., "json_parse", "number_format")
     */
    public void recordParseError(ExchangeId exchangeId, String errorType) {
        Counter.builder("exchange.messages.errors")
                .description("Message parse errors by type")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .tag(TAG_ERROR_TYPE, errorType)
                .register(registry)
                .increment();
    }

    /**
     * Records a reconnection attempt to an exchange.
     *
     * <p>Each increment means the connector lost its WebSocket connection and is
     * retrying. A sudden burst of reconnections across all exchanges suggests a
     * network issue; reconnections on a single exchange suggest the exchange is
     * having problems. In Grafana, alert on {@code rate(exchange_reconnections_total[5m]) > 0}.</p>
     *
     * @param exchangeId the exchange being reconnected to
     */
    public void recordReconnection(ExchangeId exchangeId) {
        Counter.builder("exchange.reconnections")
                .description("Reconnection attempts to exchange WebSocket")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .increment();
    }

    // ========================================================================
    // DURATION — How long does processing take?
    // ========================================================================

    /**
     * Records the time taken to parse a single WebSocket message (T1 - T0).
     *
     * <p>This is the most latency-critical metric in the connector pipeline. T0 is
     * captured as the first operation in the message handler ({@code System.nanoTime()}).
     * T1 is captured after BigDecimal conversion and NormalisedTick construction.</p>
     *
     * <p>Micrometer's {@link Timer} automatically tracks count, total time, and max.
     * With Prometheus + Grafana, you get histograms for p50/p95/p99/p999 latency.
     * Target: p99 < 50µs for JSON parse + BigDecimal conversion.</p>
     *
     * @param exchangeId    the exchange whose message was parsed
     * @param durationNanos T1 - T0 in nanoseconds
     */
    public void recordParseDuration(ExchangeId exchangeId, long durationNanos) {
        Timer.builder("exchange.parse.duration")
                .description("Time to parse a WebSocket message into NormalisedTick")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .record(Duration.ofNanos(durationNanos));
    }

    // ========================================================================
    // GAUGES — What is the current state?
    // ========================================================================

    /**
     * Updates the current feed status gauge for an exchange.
     *
     * <p>Feed status is reported as an integer ordinal:
     * CONNECTED=0, RECONNECTING=1, STALE=2, DISCONNECTED=3.
     * In Grafana, use value mappings to show the status name instead of the number.</p>
     *
     * <p><b>Why a gauge, not a counter?</b> Feed status is a point-in-time value
     * (what is the status RIGHT NOW?), not a cumulative total. Gauges report the
     * current value on each Prometheus scrape. Counters always increase.</p>
     *
     * <p><b>Lazy registration:</b> The gauge is registered on first call per exchange
     * and backed by an {@link AtomicInteger}. Subsequent calls update the integer;
     * the gauge just reads it. This avoids registering gauges for exchanges that
     * are disabled in configuration.</p>
     *
     * @param exchangeId the exchange whose status changed
     * @param status     the new feed status
     */
    public void recordFeedStatus(ExchangeId exchangeId, FeedStatus status) {
        AtomicInteger gauge = feedStatusGauges.computeIfAbsent(exchangeId, id -> {
            AtomicInteger value = new AtomicInteger(status.ordinal());
            registry.gauge("exchange.feed.status",
                    io.micrometer.core.instrument.Tags.of(TAG_EXCHANGE, id.name()),
                    value);
            log.debug("Registered feed status gauge for exchange={}", id.name());
            return value;
        });
        gauge.set(status.ordinal());
    }
}
