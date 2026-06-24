package com.arbitrage.detection.config;

import com.arbitrage.common.model.NormalisedTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the Detection Engine.
 *
 * <p>Consumes unified tick data from the normalisation pipeline:
 * <ul>
 *   <li>{@code normalised-ticks} — output of the normalisation engine, all exchanges merged</li>
 * </ul>
 *
 * <p><b>Consumer group:</b> {@code detection-engine-group} — every detection engine instance
 * participates in this group. Kafka distributes partitions across all instances in the group,
 * enabling horizontal scaling of the detection stage.
 *
 * <p><b>Why {@code auto-offset-reset=latest}?</b> The detection engine only cares about
 * current prices. Replaying historical normalised ticks on startup would flood Redis with
 * stale state and trigger false positives against current-price comparisons. Starting from
 * "now" ensures the Redis price state is always fresh.
 *
 * <p><b>Why {@code enable-auto-commit=false}?</b> Manual commit gives fine-grained
 * control over exactly when a tick is considered "processed." The offset is committed only
 * after the price state has been written to Redis and any opportunity has been published.
 * If the detection engine crashes between consuming a tick and updating Redis, the tick
 * is reprocessed on restart — avoiding silent price-state gaps.
 *
 * <p><b>Why {@code max-poll-records=1000}?</b> The detection comparison (two BigDecimal
 * subtractions + a compareTo) is ~microseconds. At 3000 ticks/sec across all exchanges,
 * 1000 records per poll absorbs ~333ms of bursts before applying backpressure. This is
 * higher than the normalisation engine's 500 because detection has lower per-record cost.
 *
 * <p><b>Why concurrency=3?</b> {@code normalised-ticks} has 3 partitions — one logically
 * assigned per exchange (keyed by canonical trading pair). Three concurrent threads means
 * each partition has a dedicated processing thread, matching the normalisation pipeline's
 * upstream parallelism.
 *
 * <p><b>Bean naming:</b> Bean names are prefixed with {@code detection} to avoid collision
 * with {@code normalisationConsumerFactory} and {@code normalisationListenerContainerFactory}
 * when both modules are scanned by {@code DashboardApiApplication}.
 */
@Configuration
public class DetectionKafkaConsumerConfig {

    /** Consumer group identifier for all detection engine instances. */
    public static final String CONSUMER_GROUP = "detection-engine-group";

    /** Normalised tick topic produced by the normalisation engine. */
    public static final String TOPIC_NORMALISED_TICKS = "normalised-ticks";

    /**
     * Creates a consumer factory for deserialising unified {@link NormalisedTick} messages.
     *
     * <p>Messages on {@code normalised-ticks} are serialised by
     * {@link org.springframework.kafka.support.serializer.JsonSerializer} in the normalisation engine.
     * We deserialise symmetrically with {@link JsonDeserializer}.
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties (bootstrap-servers, etc.)
     * @param objectMapper    shared Jackson ObjectMapper with JavaTimeModule for Instant support
     * @return configured ConsumerFactory for NormalisedTick messages
     */
    @Bean
    public ConsumerFactory<String, NormalisedTick> detectionConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        // --- Consumer Group ---
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        // --- Offset Strategy ---
        // latest: only process current prices — no historical replay on startup
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // --- Manual Commit Control ---
        // Commit only after Redis write + opportunity publish (prevents silent price-state gaps)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- Throughput Tuning ---
        // 1000 records per poll — detection is fast (~µs per tick), can handle larger batches
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        // --- Latency Tuning (Session 6.4) ---
        // Default fetch.max.wait.ms=500: consumer waits up to 500ms for enough data before returning.
        // At ~90 ticks/sec the broker fills a fetch quickly, but the 500ms cap adds worst-case latency
        // to NORMALISER_TO_DETECTOR_TRANSIT when the topic is momentarily quiet (after burst gaps).
        // Reducing to 10ms means the consumer polls at most 10ms after the last batch was empty,
        // keeping transit latency tight without burning unnecessary CPU on a tight-loop poll.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10);

        // --- Deserialization ---
        JsonDeserializer<NormalisedTick> valueDeserializer =
                new JsonDeserializer<>(NormalisedTick.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    /**
     * Creates a {@link ConcurrentKafkaListenerContainerFactory} for {@code @KafkaListener} methods
     * in the detection engine.
     *
     * <p><b>AckMode.MANUAL_IMMEDIATE:</b> Commits the offset immediately when
     * {@code acknowledgment.acknowledge()} is called. The {@code DetectionService} (Session 3.3)
     * will acknowledge after both the Redis price-state write and any opportunity publication
     * complete, ensuring the offset is never committed for a partially processed tick.
     *
     * @param detectionConsumerFactory the configured consumer factory
     * @return a container factory for detection listener registration
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalisedTick> detectionListenerContainerFactory(
            ConsumerFactory<String, NormalisedTick> detectionConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NormalisedTick> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(detectionConsumerFactory);

        // 3 threads — one per normalised-ticks partition (matches upstream per-exchange parallelism)
        factory.setConcurrency(3);

        // Manual offset commit — acknowledge only after Redis write + opportunity publish
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
