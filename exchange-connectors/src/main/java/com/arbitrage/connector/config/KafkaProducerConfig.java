package com.arbitrage.connector.config;

import com.arbitrage.common.model.NormalisedTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration tuned for low-latency raw tick publishing.
 *
 * <p><b>Why a dedicated config?</b> Spring Boot's auto-configuration provides sensible
 * defaults, but raw tick data has specific tradeoffs:</p>
 * <ul>
 *   <li><b>acks=1:</b> Wait for leader acknowledgment only — not all replicas. Adds
 *       ~2-5ms vs acks=all which adds ~10-30ms. Raw ticks are ephemeral (re-receivable
 *       from the exchange), so latency wins over durability. Trade execution confirmations
 *       would use acks=all.</li>
 *   <li><b>linger.ms=5:</b> Wait up to 5ms to batch messages. At 1000 ticks/second,
 *       this batches ~5 messages per send, reducing network round-trips by ~80%.
 *       The 5ms added latency is within our 50ms end-to-end target.</li>
 *   <li><b>batch.size=32768:</b> 32KB batch buffer — enough for ~100 serialized ticks.
 *       If the batch fills before linger.ms expires, it sends immediately.</li>
 *   <li><b>compression.type=lz4:</b> LZ4 is the fastest compression codec Kafka supports.
 *       JSON tick payloads compress ~60-70%, reducing network bandwidth with minimal
 *       CPU overhead (~0.1ms per batch). Snappy is similar; gzip is slower but compresses
 *       better — not worth the CPU cost on the hot path.</li>
 * </ul>
 *
 * <p><b>See:</b> ADR-008 (Kafka acks latency tradeoff) for the full decision record.</p>
 *
 * @see com.arbitrage.connector.binance.BinanceTickKafkaProducer for the producer that uses this config
 */
@Configuration
public class KafkaProducerConfig {

    /**
     * Creates a producer factory with low-latency settings for raw tick data.
     *
     * <p>Reads bootstrap servers from Spring's {@link KafkaProperties} (bound from
     * {@code spring.kafka.bootstrap-servers} in application.yml) and overlays
     * tick-specific producer tuning.</p>
     *
     * @param kafkaProperties Spring Boot's auto-bound Kafka properties
     * @param objectMapper    Jackson ObjectMapper for JSON serialization
     * @return a configured ProducerFactory for NormalisedTick messages
     */
    @Bean
    public ProducerFactory<String, NormalisedTick> tickProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        // --- Serialization ---
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // --- Latency vs Durability Tradeoff (see ADR-008) ---
        // acks=1: leader-only acknowledgment. ~2-5ms latency.
        // acks=all: all replicas. ~10-30ms latency. Use for trade confirmations, not raw ticks.
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // --- Batching for Throughput ---
        // Wait up to 5ms to accumulate a batch. At 1000 msgs/sec → ~5 msgs/batch.
        // Reduces network calls by ~80% with only 5ms added latency.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // 32KB batch buffer — fits ~100 serialized ticks. Sends early if full.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        // --- Compression ---
        // LZ4: fastest codec. JSON compresses ~60-70%. Minimal CPU overhead.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // --- Reliability ---
        // Retry up to 3 times on transient failures (leader election, network blip).
        // Combined with acks=1, this handles most transient broker issues.
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Idempotent producer requires acks=all; since we use acks=1, disable it.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        DefaultKafkaProducerFactory<String, NormalisedTick> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));

        return factory;
    }

    /**
     * Creates a KafkaTemplate for publishing NormalisedTick messages.
     *
     * <p>The template is thread-safe and reusable — shared across all exchange
     * producers. It wraps the producer factory and provides a high-level API
     * for sending messages with futures for async result handling.</p>
     *
     * @param producerFactory the configured producer factory
     * @return a KafkaTemplate for tick publishing
     */
    @Bean
    public KafkaTemplate<String, NormalisedTick> tickKafkaTemplate(
            @Qualifier("tickProducerFactory") ProducerFactory<String, NormalisedTick> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
