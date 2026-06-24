package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.util.KafkaHeaderUtils;
import com.arbitrage.normalisation.config.NormalisationKafkaConsumerConfig;
import com.arbitrage.normalisation.config.NormalisationKafkaProducerConfig;
import com.arbitrage.normalisation.config.NormalisationProperties;
import com.arbitrage.normalisation.transformer.TickTransformerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Core Kafka pipeline for the Normalisation Engine.
 *
 * <p>Consumes raw ticks from all three exchange-specific topics, transforms them
 * into validated {@link NormalisedTick} records with correct timestamps, and
 * publishes the results to the unified {@code normalised-ticks} topic.
 *
 * <p><b>Timestamp chain (what this service is responsible for):</b>
 * <ul>
 *   <li><b>T0–T2</b> — set by the exchange connector, read from inbound Kafka headers</li>
 *   <li><b>T3</b> ({@code normalisationStartNanos}) — captured as the first line of
 *       {@link #onRawTick(ConsumerRecord, Acknowledgment)}, representing when the record was polled</li>
 *   <li><b>T4</b> ({@code processedTimestamp}) — captured inside the transformer after validation</li>
 *   <li><b>T5</b> — captured just before the normalised tick is sent to Kafka</li>
 *   <li>All T0–T5 values are forwarded as Kafka headers on the outbound {@code normalised-ticks} message</li>
 * </ul>
 *
 * <p><b>At-least-once semantics:</b> Offsets are committed only after successful
 * produce to {@code normalised-ticks}. If the produce fails (broker unavailable,
 * serialization error), the offset is NOT committed and Kafka redelivers the raw tick
 * on the next poll. This guarantees that no valid raw tick is silently lost.
 *
 * <p><b>Why dropped ticks are still acknowledged:</b> Structurally invalid ticks
 * (null prices, unknown exchange) cannot be fixed by retrying — they will produce
 * the same result. Blocking the pipeline on bad data would stall all ticks from
 * that partition. Acknowledge + metric is the correct response; the operator is
 * alerted via the {@code normalisation.ticks.dropped} counter.
 *
 * <p><b>Threading model:</b> Three independent {@code @KafkaListener} methods — one per raw-ticks
 * topic — each backed by a single consumer thread from {@code normalisationListenerContainerFactory}.
 * Each thread processes ticks from one exchange serially (Binance, Bybit, KuCoin independently).
 * The transformer and metrics calls are stateless per-call, so no synchronisation is needed here.
 * A single multi-topic listener with {@code concurrency=3} was previously used but caused
 * {@code RangeAssignor} to assign all 3 partitions to one thread, starving Binance.
 *
 * @see TickTransformerFactory routes each tick to the correct exchange transformer
 * @see NormalisationMetrics records RED metrics for this pipeline stage
 */
@Service
@Slf4j
public class NormalisationService {

    private static final long NS_PER_MS = 1_000_000L;

    private final TickTransformerFactory transformerFactory;
    private final NormalisationMetrics metrics;
    private final FeedHealthMonitor feedHealthMonitor;
    private final ClockSkewMonitor clockSkewMonitor;
    private final NormalisationProperties normalisationProperties;

    /**
     * The {@code @Qualifier} selects the normalisation-engine's own KafkaTemplate,
     * distinct from the exchange-connectors' {@code tickKafkaTemplate}. Both produce
     * the same type ({@code NormalisedTick}) but to different topics.
     *
     * <p>The constructor is written manually (not via Lombok's {@code @RequiredArgsConstructor})
     * because {@code @Qualifier} must appear on the constructor parameter for Spring to
     * respect it during dependency injection. Lombok does not propagate field annotations
     * to generated constructor parameters by default.</p>
     */
    private final KafkaTemplate<String, NormalisedTick> kafkaTemplate;

    /**
     * Creates the NormalisationService with its required dependencies.
     *
     * @param transformerFactory routes inbound ticks to exchange-specific transformers
     * @param metrics            RED metrics recorder for this pipeline stage
     * @param feedHealthMonitor  tracks per-exchange feed health based on tick arrivals
     * @param clockSkewMonitor   tracks rolling offset between exchange and local clocks
     * @param kafkaTemplate      the normalisation-engine's KafkaTemplate for {@code normalised-ticks}
     */
    public NormalisationService(
            TickTransformerFactory transformerFactory,
            NormalisationMetrics metrics,
            FeedHealthMonitor feedHealthMonitor,
            ClockSkewMonitor clockSkewMonitor,
            NormalisationProperties normalisationProperties,
            @Qualifier("normalisedTickKafkaTemplate") KafkaTemplate<String, NormalisedTick> kafkaTemplate) {
        this.transformerFactory = transformerFactory;
        this.metrics = metrics;
        this.feedHealthMonitor = feedHealthMonitor;
        this.clockSkewMonitor = clockSkewMonitor;
        this.normalisationProperties = normalisationProperties;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Consumes a raw tick from any of the three exchange raw-tick topics and normalises it.
     *
     * <p><b>Processing steps:</b>
     * <ol>
     *   <li>Capture T3 ({@code normalisationStartNanos}) — Kafka poll timestamp</li>
     *   <li>Guard: null tick → drop with {@code null_tick} reason, acknowledge, return</li>
     *   <li>Route through {@link TickTransformerFactory}: transformer validates fields and
     *       stamps T4 ({@code processedTimestamp}) on the outbound tick</li>
     *   <li>If transformer returns empty → drop with appropriate reason, acknowledge, return</li>
     *   <li>Record tick arrival in {@link FeedHealthMonitor} — marks feed as CONNECTED</li>
     *   <li>Record exchange timestamp offset in {@link ClockSkewMonitor} — updates EWMA, warns on jumps</li>
     *   <li>Record T4-T3 latency via {@link NormalisationMetrics#recordProcessingDuration}</li>
     *   <li>Publish to {@code normalised-ticks} with key = "EXCHANGE:PAIR" (e.g. "BINANCE:BTC-USDT")</li>
     *   <li>On produce success → record normalised counter, acknowledge offset (T5)</li>
     *   <li>On produce failure → log error, do NOT acknowledge (Kafka redelivers)</li>
     * </ol>
     *
     * <p><b>Key = "{@code exchangeId}:{@link com.arbitrage.common.model.TradingPair#canonicalSymbol()}"
     * (e.g. "BINANCE:BTC-USDT").</b>
     * Using exchange+pair as the Kafka message key distributes the 9 independent producer streams
     * (3 exchanges × 3 pairs) evenly across all partitions in the {@code normalised-ticks} topic.
     * The detection engine stores and reads per-exchange prices via Redis, so same-partition
     * co-location of different exchanges is not required for correctness.
     *
     * @param inboundTick    the raw tick deserialized from Kafka; may be null on deserialization failure
     * @param acknowledgment manual offset commit handle (AckMode.MANUAL_IMMEDIATE)
     */
    /**
     * Kafka entry point for Binance raw ticks.
     *
     * <p>Separate {@code @KafkaListener} per topic ensures each exchange gets its own dedicated
     * consumer thread. A single multi-topic listener with {@code concurrency=3} caused Kafka's
     * {@code RangeAssignor} to assign all 3 single-partition topics to ONE thread, leaving the
     * other 2 idle. This caused Binance (highest tick rate) to starve while the thread processed
     * Bybit/KuCoin batches, pushing it above the 5-second STALE threshold intermittently.
     *
     * @param record         the raw Kafka consumer record including headers and value
     * @param acknowledgment manual offset commit handle
     */
    @KafkaListener(topics = NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                   containerFactory = "normalisationListenerContainerFactory")
    public void onRawTickBinance(ConsumerRecord<String, NormalisedTick> record, Acknowledgment acknowledgment) {
        onRawTick(record.value(), acknowledgment, record.headers());
    }

    /**
     * Kafka entry point for Bybit raw ticks. See {@link #onRawTickBinance} for threading rationale.
     *
     * @param record         the raw Kafka consumer record including headers and value
     * @param acknowledgment manual offset commit handle
     */
    @KafkaListener(topics = NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT,
                   containerFactory = "normalisationListenerContainerFactory")
    public void onRawTickBybit(ConsumerRecord<String, NormalisedTick> record, Acknowledgment acknowledgment) {
        onRawTick(record.value(), acknowledgment, record.headers());
    }

    /**
     * Kafka entry point for KuCoin raw ticks. See {@link #onRawTickBinance} for threading rationale.
     *
     * @param record         the raw Kafka consumer record including headers and value
     * @param acknowledgment manual offset commit handle
     */
    @KafkaListener(topics = NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN,
                   containerFactory = "normalisationListenerContainerFactory")
    public void onRawTickKuCoin(ConsumerRecord<String, NormalisedTick> record, Acknowledgment acknowledgment) {
        onRawTick(record.value(), acknowledgment, record.headers());
    }

    /**
     * Test-facing overload — uses empty headers (all T0–T2 values will be 0).
     * Existing unit tests call this overload directly without requiring header setup.
     *
     * @param inboundTick    the raw tick; may be null on deserialization failure
     * @param acknowledgment manual offset commit handle
     */
    public void onRawTick(NormalisedTick inboundTick, Acknowledgment acknowledgment) {
        onRawTick(inboundTick, acknowledgment, new RecordHeaders());
    }

    /**
     * Core processing logic — shared by both entry points.
     *
     * @param inboundTick    the raw tick; may be null on deserialization failure
     * @param acknowledgment manual offset commit handle
     * @param inboundHeaders Kafka headers from the raw-tick record (carries T0–T2)
     */
    private void onRawTick(NormalisedTick inboundTick, Acknowledgment acknowledgment, Headers inboundHeaders) {
        // T3: timestamp this record was polled from Kafka — first operation, no exceptions
        final long normalisationStartNanos = System.nanoTime();

        // --- Guard: null tick (deserialization failure) ---
        if (inboundTick == null) {
            log.warn("Received null tick from Kafka — dropping. This may indicate a deserialization problem.");
            metrics.recordDroppedTick(null, NormalisationMetrics.DROP_REASON_NULL_TICK);
            acknowledgment.acknowledge();
            return;
        }

        // --- Staleness pre-filter — drop backlog ticks before any transform work ---
        // receivedTimestamp is System.nanoTime() from the connector (same JVM). Ticks older
        // than the threshold are from Kafka backlog (load tests, crash recovery, burst lag).
        // Dropping stale ticks here burns through backlog in ~0.01ms/message vs ~1ms/message
        // for a full transform+publish, clearing a 100k backlog in ~1s vs ~100s.
        long ageNs = normalisationStartNanos - inboundTick.getReceivedTimestamp();
        long thresholdNs = normalisationProperties.getStalenessThresholdMs() * NS_PER_MS;
        if (ageNs > thresholdNs) {
            long ageMs = ageNs / NS_PER_MS;
            log.debug("Stale raw tick dropped: exchange={} pair={} ageMs={} thresholdMs={}",
                    inboundTick.getExchangeId(), inboundTick.getTradingPair(),
                    ageMs, normalisationProperties.getStalenessThresholdMs());
            metrics.recordDroppedTick(inboundTick.getExchangeId(), NormalisationMetrics.DROP_REASON_STALE_TICK);
            acknowledgment.acknowledge();
            return;
        }

        // --- Route through the transformer ---
        Optional<NormalisedTick> transformResult = transformerFactory.transform(inboundTick, normalisationStartNanos);

        if (transformResult.isEmpty()) {
            String dropReason = transformerFactory.supports(inboundTick.getExchangeId())
                    ? NormalisationMetrics.DROP_REASON_INVALID_FIELDS
                    : NormalisationMetrics.DROP_REASON_NO_TRANSFORMER;
            log.warn("Tick dropped: exchange={} pair={} reason={}",
                    inboundTick.getExchangeId(), inboundTick.getTradingPair(), dropReason);
            metrics.recordDroppedTick(inboundTick.getExchangeId(), dropReason);
            acknowledgment.acknowledge();
            return;
        }

        NormalisedTick normalisedTick = transformResult.get();

        // --- Update feed health and clock skew — both track data arrival, not publish success ---
        feedHealthMonitor.recordTickReceived(normalisedTick.getExchangeId());
        clockSkewMonitor.recordTick(normalisedTick.getExchangeId(), normalisedTick.getExchangeTimestamp());

        // --- Record T4-T3 latency (pure transformer work) ---
        final long t4MinusT3 = normalisedTick.getProcessedTimestamp() - normalisationStartNanos;
        metrics.recordProcessingDuration(normalisedTick.getExchangeId(), t4MinusT3);

        log.debug("Tick normalised: exchange={} pair={} bid={} ask={} latencyNanos={}",
                normalisedTick.getExchangeId(),
                normalisedTick.getTradingPair(),
                normalisedTick.getBestBidPrice(),
                normalisedTick.getBestAskPrice(),
                t4MinusT3);

        // --- Publish to normalised-ticks; acknowledge only on success (at-least-once) ---
        final String messageKey = normalisedTick.getExchangeId() + ":" + normalisedTick.getTradingPair().canonicalSymbol();

        // T5: captured just before send — measures normaliser-side produce overhead
        final long t5 = System.nanoTime();

        final ProducerRecord<String, NormalisedTick> outbound =
                new ProducerRecord<>(NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS, messageKey, normalisedTick);
        // Forward T0–T2 from inbound headers; stamp T3–T5 from this normalisation pass
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T0,
                KafkaHeaderUtils.read(inboundHeaders, KafkaHeaderUtils.HDR_T0));
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T1,
                KafkaHeaderUtils.read(inboundHeaders, KafkaHeaderUtils.HDR_T1));
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T2,
                KafkaHeaderUtils.read(inboundHeaders, KafkaHeaderUtils.HDR_T2));
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T3, normalisationStartNanos);
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T4, normalisedTick.getProcessedTimestamp());
        KafkaHeaderUtils.write(outbound.headers(), KafkaHeaderUtils.HDR_T5, t5);

        kafkaTemplate.send(outbound)
                .whenComplete((sendResult, exception) -> {
                    if (exception != null) {
                        // Do NOT acknowledge — Kafka will redeliver the raw tick
                        log.error("Failed to publish normalised tick: exchange={} pair={} error={}",
                                normalisedTick.getExchangeId(),
                                normalisedTick.getTradingPair(),
                                exception.getMessage());
                    } else {
                        // T5: offset committed after successful produce
                        metrics.recordNormalisedTick(normalisedTick.getExchangeId());
                        acknowledgment.acknowledge();
                        log.debug("Offset committed: exchange={} pair={} partition={} offset={}",
                                normalisedTick.getExchangeId(),
                                normalisedTick.getTradingPair(),
                                sendResult.getRecordMetadata().partition(),
                                sendResult.getRecordMetadata().offset());
                    }
                });
    }
}
