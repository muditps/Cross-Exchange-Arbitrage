package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.LatencyContext;
import com.arbitrage.common.util.KafkaHeaderUtils;
import com.arbitrage.detection.config.DetectionKafkaProducerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

/**
 * Publishes {@link ArbitrageOpportunity} lifecycle events to the
 * {@code arbitrage-opportunities} Kafka topic.
 *
 * <p><b>What gets published:</b> Only terminal state transitions — DETECTED (new opportunity),
 * CLOSED (spread disappeared), and EXPIRED (stale open opportunity removed by timer).
 * OPEN updates are never published — they are internal state tracked in
 * {@link OpportunityTracker} to avoid flooding downstream consumers.
 *
 * <p><b>Partition key = {@code pair.canonicalSymbol()}:</b> All events for one trading pair
 * (BTC-USDT, ETH-USDT, etc.) land on the same Kafka partition, in order.
 * This guarantees downstream consumers see DETECTED before CLOSED for the same pair.
 * All directions (BINANCE→KUCOIN, KUCOIN→BINANCE) for one pair are co-partitioned —
 * this is intentional: a consumer processing BTC-USDT opportunities sees the complete
 * picture for that pair without cross-partition ordering concerns.
 *
 * <p><b>Async produce:</b> {@link KafkaTemplate#send} is non-blocking. The result
 * is handled via {@code whenComplete} callback. Failures are logged at ERROR level
 * but do NOT throw — a lost event is re-derivable from the next profitable tick.
 */
@Service
@Slf4j
public class OpportunityKafkaPublisher {

    private final KafkaTemplate<String, ArbitrageOpportunity> kafkaTemplate;

    public OpportunityKafkaPublisher(
            @Qualifier("opportunityKafkaTemplate")
            KafkaTemplate<String, ArbitrageOpportunity> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a lifecycle event to {@code arbitrage-opportunities} with latency context.
     *
     * <p>Stamps T9 (opportunity published) and writes all T0–T9 nanosecond timestamps
     * as Kafka headers. If {@code latencyCtx} is {@code null} (expiry-sweep events),
     * headers are omitted and no latency is recorded — EXPIRED events have no pipeline origin.
     *
     * <p>The call is non-blocking. Produce errors are logged but not propagated.
     *
     * @param opportunity the DETECTED, CLOSED, or EXPIRED opportunity event to publish
     * @param latencyCtx  T0–T8 pipeline context, or {@code null} for expiry-sweep events
     * @return the T9 nanosecond timestamp stamped at produce time, so callers can build
     *         a complete {@link LatencyContext} and record the full end-to-end latency
     */
    public long publish(ArbitrageOpportunity opportunity, @Nullable LatencyContext latencyCtx) {
        final long t9 = System.nanoTime(); // T9: opportunity published to Kafka
        final String partitionKey = opportunity.getTradingPair().canonicalSymbol();

        final ProducerRecord<String, ArbitrageOpportunity> record = new ProducerRecord<>(
                DetectionKafkaProducerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES, partitionKey, opportunity);

        if (latencyCtx != null) {
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T0, latencyCtx.getT0WebSocketReceived());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T1, latencyCtx.getT1ParseStart());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T2, latencyCtx.getT2RawTickPublished());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T3, latencyCtx.getT3NormalisationReceived());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T4, latencyCtx.getT4NormalisationComplete());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T5, latencyCtx.getT5NormalisedTickPublished());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T6, latencyCtx.getT6DetectionReceived());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T7, latencyCtx.getT7RedisStateUpdated());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T8, latencyCtx.getT8ComparisonComplete());
            KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T9, t9);
        }

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish opportunity: id={} status={} pair={} error={}",
                                opportunity.getId(), opportunity.getStatus(), partitionKey, ex.getMessage(), ex);
                    } else {
                        log.debug("Published opportunity: id={} status={} pair={} partition={} offset={}",
                                opportunity.getId(), opportunity.getStatus(), partitionKey,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
        return t9;
    }
}
