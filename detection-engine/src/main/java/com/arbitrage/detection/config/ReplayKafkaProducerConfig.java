package com.arbitrage.detection.config;

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
 * Kafka producer configuration for the {@link com.arbitrage.detection.loadtest.TickReplayTool}.
 *
 * <p>Produces synthetic {@link NormalisedTick} messages directly to the {@code normalised-ticks}
 * topic, bypassing the exchange connectors and normalisation engine. This lets load tests
 * inject ticks at arbitrary rates to stress-test the detection pipeline in isolation.
 *
 * <p><b>Bean naming:</b> Uses distinct {@code replay*} names to avoid collisions with
 * {@code normalisedTickProducerFactory} / {@code normalisedTickKafkaTemplate} registered by
 * {@code NormalisationKafkaProducerConfig} when all modules are scanned by
 * {@code DashboardApiApplication}.
 *
 * <p><b>Producer tuning for high-rate injection:</b> {@code linger.ms=0} and batch size 64KB
 * maximise throughput at burst rates (10,000 msgs/sec in BURST mode). Unlike the live pipeline
 * where small batches reduce latency, the replay tool cares about injection throughput, not
 * its own produce latency.
 */
@Configuration
public class ReplayKafkaProducerConfig {

    /**
     * Creates a producer factory for injecting synthetic ticks into the detection pipeline.
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties
     * @param objectMapper    shared Jackson ObjectMapper for consistent serialization
     * @return configured ProducerFactory for NormalisedTick load-test injection
     */
    @Bean
    public ProducerFactory<String, NormalisedTick> replayNormalisedTickProducerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // acks=1: replay ticks are synthetic — loss is acceptable
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Larger batching: maximise injection throughput for burst mode
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        DefaultKafkaProducerFactory<String, NormalisedTick> factory =
                new DefaultKafkaProducerFactory<>(props);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));

        return factory;
    }

    /**
     * Creates a KafkaTemplate for injecting synthetic ticks into {@code normalised-ticks}.
     *
     * @param replayNormalisedTickProducerFactory the replay-specific producer factory
     * @return a KafkaTemplate for load-test tick injection
     */
    @Bean
    public KafkaTemplate<String, NormalisedTick> replayNormalisedTickKafkaTemplate(
            @Qualifier("replayNormalisedTickProducerFactory")
            ProducerFactory<String, NormalisedTick> replayNormalisedTickProducerFactory) {
        return new KafkaTemplate<>(replayNormalisedTickProducerFactory);
    }
}
