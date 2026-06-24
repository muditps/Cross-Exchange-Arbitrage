package com.arbitrage.normalisation.config;

import com.arbitrage.common.model.NormalisedTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for publishing normalised ticks.
 *
 * <p>Produces unified {@link NormalisedTick} messages to the {@code normalised-ticks}
 * topic. This is the single output point of the normalisation engine — all downstream
 * components (detection engine, execution simulator) consume from this topic.
 *
 * <p><b>Latency tradeoff vs raw tick producer:</b>
 * <ul>
 *   <li>Raw tick producer (exchange-connectors) uses {@code acks=1} — latency wins
 *       over durability because raw ticks are re-receivable from the exchange.</li>
 *   <li>Normalised tick producer also uses {@code acks=1} for the same reason —
 *       if a normalised tick is lost, it will be re-derived from the next raw tick.</li>
 * </ul>
 *
 * <p><b>Why a separate producer bean?</b> The exchange-connectors module already registers a
 * {@code tickProducerFactory} and {@code tickKafkaTemplate} bean. To avoid Spring's
 * "expected single matching bean but found 2" error, this config uses distinct bean names:
 * {@code normalisedTickProducerFactory} and {@code normalisedTickKafkaTemplate}.
 *
 * <p><b>See:</b> ADR-008 (Kafka acks latency tradeoff) for the full decision record.
 */
@Configuration
public class NormalisationKafkaProducerConfig {

    /** The output topic for all normalised ticks, consumed by detection-engine. */
    public static final String TOPIC_NORMALISED_TICKS = "normalised-ticks";

    /**
     * Creates a producer factory tuned for normalised tick publishing.
     *
     * <p>Mirrors the exchange-connectors producer settings: acks=1, LZ4 compression.
     * {@code linger.ms=2} (reduced from 5 in Session 6.4 — see inline comment). Normalised ticks are structurally the same as raw ticks
     * (same {@link NormalisedTick} type) so the same latency/throughput tradeoffs apply.
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties
     * @param objectMapper    shared Jackson ObjectMapper for consistent serialization
     * @return configured ProducerFactory for NormalisedTick messages
     */
    @Bean
    public ProducerFactory<String, NormalisedTick> normalisedTickProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        // --- Serialization ---
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // --- Latency vs Durability (see ADR-008) ---
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // --- Batching (Session 6.4: reduced from 5ms to 2ms) ---
        // linger.ms controls how long the producer waits to accumulate records into a batch before
        // sending. Lower = lower latency; higher = better throughput via larger batches.
        // At ~90 ticks/sec, 5ms accumulated ~1 batch every 450 ticks — far more batching than needed.
        // 2ms reduces CONNECTOR_TO_NORMALISER_TRANSIT add-latency from up to 5ms to up to 2ms.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        // --- Compression ---
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // --- Reliability ---
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        DefaultKafkaProducerFactory<String, NormalisedTick> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));

        return factory;
    }

    /**
     * Creates a KafkaTemplate for publishing to {@code normalised-ticks}.
     *
     * @param normalisedTickProducerFactory the normalisation-specific producer factory
     * @return a KafkaTemplate for normalised tick publishing
     */
    @Bean
    public KafkaTemplate<String, NormalisedTick> normalisedTickKafkaTemplate(
            @Qualifier("normalisedTickProducerFactory")
            ProducerFactory<String, NormalisedTick> normalisedTickProducerFactory) {
        return new KafkaTemplate<>(normalisedTickProducerFactory);
    }
}
