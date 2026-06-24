package com.arbitrage.normalisation.config;

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
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the Normalisation Engine.
 *
 * <p>Consumes raw tick data from three exchange-specific topics:
 * <ul>
 *   <li>{@code raw-ticks-binance}</li>
 *   <li>{@code raw-ticks-bybit}</li>
 *   <li>{@code raw-ticks-kucoin}</li>
 * </ul>
 *
 * <p><b>Consumer group:</b> {@code normalisation-engine-group} — every normalisation
 * consumer instance participates in this group. Kafka distributes partitions across
 * all instances in the group, enabling horizontal scaling of the normalisation stage.
 *
 * <p><b>Why {@code auto-offset-reset=latest}?</b> We only care about current prices —
 * replaying historical raw ticks on startup would flood the detection engine with stale
 * data. Latest means we start from "now" on first launch.
 *
 * <p><b>Why {@code enable-auto-commit=false}?</b> Manual commit control prevents
 * duplicate normalised-tick production. We commit only after successfully producing
 * to the {@code normalised-ticks} topic. If production fails, the raw tick is
 * reprocessed rather than silently lost.
 *
 * <p><b>Why {@code max-poll-records=500}?</b> At 1000 ticks/sec per exchange (3000 total),
 * 500 records per poll gives ~167ms of data per batch — enough to absorb bursts without
 * overwhelming the normalisation loop. Tune down if GC pressure is observed.
 */
@Configuration
public class NormalisationKafkaConsumerConfig {

    /** Consumer group identifier for all normalisation engine instances. */
    public static final String CONSUMER_GROUP = "normalisation-engine-group";

    /** Raw tick topics produced by the exchange connectors. */
    public static final String TOPIC_RAW_BINANCE = "raw-ticks-binance";
    public static final String TOPIC_RAW_BYBIT = "raw-ticks-bybit";
    public static final String TOPIC_RAW_KUCOIN = "raw-ticks-kucoin";

    /**
     * Creates a consumer factory for deserialising raw tick JSON strings.
     *
     * <p>Raw ticks arrive as JSON strings (key: canonical symbol, value: NormalisedTick JSON).
     * The exchange connectors serialize using {@link org.springframework.kafka.support.serializer.JsonSerializer}
     * — so we deserialize symmetrically with {@link JsonDeserializer}.
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties (bootstrap-servers, etc.)
     * @param objectMapper    shared Jackson ObjectMapper for consistent JSON handling
     * @return configured ConsumerFactory for NormalisedTick messages
     */
    @Bean
    public ConsumerFactory<String, NormalisedTick> normalisationConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        // --- Consumer Group ---
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        // --- Offset Strategy ---
        // latest: start from current position on first launch — no historical replay
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // --- Manual Commit Control ---
        // Commit only after successful downstream production (prevents silent data loss)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- Throughput Tuning ---
        // 500 records per poll — handles bursts at 3000 ticks/sec without overwhelming the loop
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        // --- Latency Tuning ---
        // Default fetch.max.wait.ms=500: broker blocks the consumer's fetch request for up to
        // 500ms when there is not enough data to satisfy fetch.min.bytes. Even at high tick rates,
        // brief inter-burst gaps cause p99 transit latency to reach 400-500ms.
        // 10ms matches the tuning already applied to the detection consumer (Session 6.4)
        // and keeps CONNECTOR_TO_NORMALISER_TRANSIT p99 in the single-digit ms range.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10);

        // --- Deserialization ---
        JsonDeserializer<NormalisedTick> valueDeserializer = new JsonDeserializer<>(NormalisedTick.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    /**
     * Creates a {@link ConcurrentKafkaListenerContainerFactory} for {@code @KafkaListener} methods.
     *
     * <p>Concurrency is set to 3 — one thread per raw-ticks topic (binance, bybit, kucoin).
     * Each thread owns one or more partitions within its assigned topic, enabling fully
     * parallel normalisation across exchanges.
     *
     * <p><b>AckMode.MANUAL_IMMEDIATE:</b> Commits the offset immediately when
     * {@code acknowledgment.acknowledge()} is called. This gives the {@code NormalisationService}
     * fine-grained control — it acknowledges after successfully producing to {@code normalised-ticks}.
     *
     * @param normalisationConsumerFactory the configured consumer factory
     * @return a container factory for listener registration
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalisedTick> normalisationListenerContainerFactory(
            ConsumerFactory<String, NormalisedTick> normalisationConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NormalisedTick> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(normalisationConsumerFactory);

        // 1 thread per container — NormalisationService registers 3 separate @KafkaListeners
        // (one per topic), so 3 independent consumer threads total. Using concurrency=3 on a
        // single multi-topic @KafkaListener caused Kafka's RangeAssignor to put ALL 3 partitions
        // on ONE thread, with the other 2 idle.
        factory.setConcurrency(1);

        // Manual offset commit — acknowledge only after successful downstream production
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
