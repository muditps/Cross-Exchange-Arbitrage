package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Micrometer-based metrics for the Normalisation Engine, following the RED pattern:
 * Rate (ticks normalised/sec), Errors (dropped ticks + reasons), Duration (T4-T3 latency).
 *
 * <p><b>Timestamp chain in the normalisation pipeline:</b>
 * <ul>
 *   <li><b>T0</b> — {@code receivedTimestamp}: when the raw WebSocket frame arrived at the connector.
 *       Set once by the connector; never overwritten.</li>
 *   <li><b>T3</b> — {@code normalisationStartNanos}: {@code System.nanoTime()} captured as the
 *       very first line of the {@code @KafkaListener}. This is when the normalisation engine
 *       polled the record off the Kafka topic.</li>
 *   <li><b>T4</b> — {@code processedTimestamp}: {@code System.nanoTime()} captured inside the
 *       transformer after all field validation and tick construction is complete.</li>
 *   <li><b>T5</b> — implied by Kafka produce callback: when the normalised tick was
 *       successfully produced to the {@code normalised-ticks} topic and the offset committed.</li>
 * </ul>
 *
 * <p><b>Why T4-T3?</b> This delta measures pure normalisation work — field validation,
 * tick reconstruction, processedTimestamp stamping. It isolates the transformer from
 * Kafka poll overhead (T3 start) and Kafka produce overhead (T5 end). Target: p99 &lt; 50µs.
 *
 * <p><b>Drop reasons (normalisation.ticks.dropped tag "reason"):</b>
 * <ul>
 *   <li>{@code no_transformer} — no TickTransformer registered for this ExchangeId</li>
 *   <li>{@code invalid_fields} — transformer returned empty (null prices, null pair, etc.)</li>
 *   <li>{@code null_tick} — inbound tick was null (deserialization edge case)</li>
 * </ul>
 *
 * @see ConnectorMetrics the equivalent metrics class for the exchange-connectors module
 */
@Component
@Slf4j
public class NormalisationMetrics {

    /** Tag key applied to all meters for per-exchange filtering in Prometheus/Grafana. */
    private static final String TAG_EXCHANGE = "exchange";

    /**
     * Tag key for drop reason — enables queries like
     * {@code rate(normalisation_ticks_dropped_total{reason="invalid_fields"}[1m])}.
     */
    private static final String TAG_REASON = "reason";

    /** Drop reason: inbound tick was null (defensive; should not occur in normal flow). */
    public static final String DROP_REASON_NULL_TICK = "null_tick";

    /** Drop reason: no TickTransformer is registered for the tick's ExchangeId. */
    public static final String DROP_REASON_NO_TRANSFORMER = "no_transformer";

    /** Drop reason: transformer returned empty — invalid fields (null prices, null pair, etc.). */
    public static final String DROP_REASON_INVALID_FIELDS = "invalid_fields";

    /**
     * Drop reason: raw tick is older than {@code arbitrage.normalisation.staleness-threshold-ms}.
     *
     * <p>These are backlog messages from Kafka (load tests, restart lag, burst traffic).
     * Dropping them here prevents flooding the detection engine with stale prices.
     */
    public static final String DROP_REASON_STALE_TICK = "stale_tick";

    private final MeterRegistry registry;

    /**
     * Creates normalisation metrics and registers them with the Micrometer registry.
     *
     * @param registry the Micrometer meter registry, auto-injected by Spring Boot
     */
    public NormalisationMetrics(MeterRegistry registry) {
        this.registry = registry;
        log.info("NormalisationMetrics initialised with registry: {}", registry.getClass().getSimpleName());
    }

    // ========================================================================
    // RATE — How many ticks are flowing through normalisation?
    // ========================================================================

    /**
     * Records a tick that was successfully transformed and published to {@code normalised-ticks}.
     *
     * <p>This counter increments at T5 (after the Kafka produce callback succeeds).
     * Use {@code rate(normalisation_ticks_normalised_total{exchange="BINANCE"}[1m])} in
     * Prometheus to derive throughput per exchange.</p>
     *
     * @param exchangeId the exchange that produced the tick
     */
    public void recordNormalisedTick(ExchangeId exchangeId) {
        Counter.builder("normalisation.ticks.normalised")
                .description("Ticks successfully normalised and published to normalised-ticks topic")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .increment();
    }

    // ========================================================================
    // ERRORS — What is being dropped and why?
    // ========================================================================

    /**
     * Records a tick that was dropped without being published to {@code normalised-ticks}.
     *
     * <p>Dropped ticks are still acknowledged — they are not retried because retrying
     * a structurally invalid tick would produce the same result. The {@code reason} tag
     * distinguishes the drop cause so Grafana alerts can target specific failure modes.
     *
     * <p>Expected drop rates:
     * <ul>
     *   <li>{@code null_tick} — should be near zero; indicates a serialization problem</li>
     *   <li>{@code no_transformer} — should be zero unless an unknown exchange is connected</li>
     *   <li>{@code invalid_fields} — some ticks from exchanges may have missing fields; alert
     *       if rate exceeds 1% of total ticks for a given exchange</li>
     * </ul>
     *
     * @param exchangeId the exchange that produced the dropped tick (may be null — logged as "UNKNOWN")
     * @param reason     why the tick was dropped (use the {@code DROP_REASON_*} constants)
     */
    public void recordDroppedTick(ExchangeId exchangeId, String reason) {
        String exchangeTag = exchangeId != null ? exchangeId.name() : "UNKNOWN";
        Counter.builder("normalisation.ticks.dropped")
                .description("Ticks dropped during normalisation without being published")
                .tag(TAG_EXCHANGE, exchangeTag)
                .tag(TAG_REASON, reason)
                .register(registry)
                .increment();
    }

    // ========================================================================
    // DURATION — How long does normalisation take?
    // ========================================================================

    /**
     * Records the time between Kafka poll (T3) and transformer completion (T4).
     *
     * <p>This is the pure normalisation latency — field validation + NormalisedTick
     * reconstruction + processedTimestamp stamping. It excludes Kafka poll overhead
     * (network + deserialization) and Kafka produce overhead (batching + broker ack).</p>
     *
     * <p>Call this method after the transformer returns a non-empty result, using
     * {@code normalisedTick.getProcessedTimestamp() - normalisationStartNanos} as
     * the duration. Both values are {@code System.nanoTime()} captures in the same JVM,
     * so the subtraction is valid and monotonic.</p>
     *
     * <p>Prometheus histogram: {@code histogram_quantile(0.99, normalisation_processing_duration_seconds_bucket)}
     * gives p99 normalisation latency. Target: p99 &lt; 50µs.</p>
     *
     * @param exchangeId         the exchange whose tick was transformed
     * @param durationNanos      T4 - T3 in nanoseconds
     */
    public void recordProcessingDuration(ExchangeId exchangeId, long durationNanos) {
        Timer.builder("normalisation.processing.duration")
                .description("Time from Kafka poll (T3) to transformer completion (T4)")
                .tag(TAG_EXCHANGE, exchangeId.name())
                .register(registry)
                .record(Duration.ofNanos(durationNanos));
    }
}
