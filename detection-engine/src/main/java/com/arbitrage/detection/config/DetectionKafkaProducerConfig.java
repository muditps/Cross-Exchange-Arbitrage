package com.arbitrage.detection.config;

import com.arbitrage.common.model.ArbitrageOpportunity;
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
 * Kafka producer configuration for publishing {@link ArbitrageOpportunity} lifecycle events.
 *
 * <p>Produces to the {@code arbitrage-opportunities} topic on three lifecycle transitions:
 * <ul>
 *   <li><b>DETECTED</b> — first tick with net spread &gt; 0 for a given direction</li>
 *   <li><b>CLOSED</b> — first tick where net spread drops to ≤ 0 after being OPEN</li>
 *   <li><b>EXPIRED</b> — opportunity OPEN for longer than the configured max duration (60s default)</li>
 * </ul>
 *
 * <p>OPEN updates (intermediate ticks while a spread persists) are NOT published — they are
 * tracked silently in {@link com.arbitrage.detection.service.OpportunityTracker} to avoid
 * flooding downstream consumers with thousands of updates per minute.
 *
 * <p><b>Why {@code acks=1}:</b> Consistency with the upstream producers (exchange-connectors,
 * normalisation-engine). Opportunity events are derived data — if a DETECTED event is lost,
 * the detection engine will re-detect the opportunity on the next profitable tick. CLOSED
 * and EXPIRED events are also re-derivable from the sequence of ticks. The latency cost of
 * {@code acks=all} is not justified for a derived event stream.
 *
 * <p><b>Bean naming:</b> Beans are prefixed {@code opportunity} to avoid collision with
 * {@code normalisedTickProducerFactory} and {@code tickProducerFactory} when all modules
 * are scanned by {@code DashboardApiApplication}.
 */
@Configuration
public class DetectionKafkaProducerConfig {

    /** Topic for arbitrage opportunity lifecycle events, consumed by execution-simulator. */
    public static final String TOPIC_ARBITRAGE_OPPORTUNITIES = "arbitrage-opportunities";

    /**
     * Creates a producer factory for serialising {@link ArbitrageOpportunity} as JSON.
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties (bootstrap-servers, etc.)
     * @param objectMapper    shared Jackson ObjectMapper with JavaTimeModule for Instant support
     * @return configured ProducerFactory for ArbitrageOpportunity messages
     */
    @Bean
    public ProducerFactory<String, ArbitrageOpportunity> opportunityProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Latency-first: at-least-once is sufficient for derived event data
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Small batching — opportunities are rare events, not a high-rate stream
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        DefaultKafkaProducerFactory<String, ArbitrageOpportunity> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));

        return factory;
    }

    /**
     * Creates a KafkaTemplate for publishing to {@code arbitrage-opportunities}.
     *
     * @param opportunityProducerFactory the opportunity-specific producer factory
     * @return a KafkaTemplate for ArbitrageOpportunity publishing
     */
    @Bean
    public KafkaTemplate<String, ArbitrageOpportunity> opportunityKafkaTemplate(
            @Qualifier("opportunityProducerFactory")
            ProducerFactory<String, ArbitrageOpportunity> opportunityProducerFactory) {
        return new KafkaTemplate<>(opportunityProducerFactory);
    }
}
