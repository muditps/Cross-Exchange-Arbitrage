package com.arbitrage.detection;

import com.arbitrage.detection.config.DetectionKafkaConsumerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the Detection Engine module wires up correctly.
 *
 * <p>Uses embedded Kafka (no Docker required) to test that the consumer bean
 * is created with the expected configuration. This is a "context loads" test —
 * it proves the Spring context assembles without errors and the consumer has the
 * correct group ID, commit strategy, and ack mode set.
 *
 * <p><b>Why {@code @Import(KafkaAutoConfiguration.class)}?</b> This is a library
 * module — there is no {@code @SpringBootApplication} in src/main. The Kafka config
 * beans depend on {@link org.springframework.boot.autoconfigure.kafka.KafkaProperties},
 * which is registered by {@code KafkaAutoConfiguration}. Importing it explicitly
 * avoids a full Spring Boot startup while providing exactly what the consumer factory needs.
 *
 * <p><b>Why embedded Kafka?</b> Consumer factories validate bootstrap-servers at
 * context creation. {@code @EmbeddedKafka} provides an in-process broker in milliseconds —
 * no Docker, no Testcontainers overhead. The full pipeline integration test
 * (Session 3.8) will use Testcontainers for real broker behaviour.
 */
@SpringBootTest(classes = {
        DetectionEngineApplicationTest.TestConfig.class,
        DetectionKafkaConsumerConfig.class
})
@Import(KafkaAutoConfiguration.class)
@EmbeddedKafka(
        partitions = 3,
        topics = {DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS}
)
@ActiveProfiles("test")
class DetectionEngineApplicationTest {

    /**
     * Provides beans not available in the minimal test context.
     * ObjectMapper is normally registered by JacksonAutoConfiguration — we provide
     * it explicitly here to keep the test focused without importing full auto-config.
     */
    @TestConfiguration
    static class TestConfig {
        @Bean
        public ObjectMapper objectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper;
        }
    }

    @Autowired
    private ConsumerFactory<String, ?> detectionConsumerFactory;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, ?> detectionListenerContainerFactory;

    // ─── Consumer factory tests ───────────────────────────────────────────────

    @Test
    @DisplayName("Consumer factory uses detection-engine-group")
    void consumerFactory_hasCorrectGroupId() {
        Map<String, Object> props = detectionConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG))
                .isEqualTo(DetectionKafkaConsumerConfig.CONSUMER_GROUP);
    }

    @Test
    @DisplayName("Consumer factory disables auto-commit for manual offset control")
    void consumerFactory_hasManualCommit() {
        Map<String, Object> props = detectionConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
    }

    @Test
    @DisplayName("Consumer factory sets max-poll-records to 1000")
    void consumerFactory_hasMaxPollRecords() {
        Map<String, Object> props = detectionConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(1000);
    }

    // ─── Listener container factory tests ─────────────────────────────────────

    @Test
    @DisplayName("Listener container factory is created and non-null")
    void listenerContainerFactory_isCreated() {
        assertThat(detectionListenerContainerFactory).isNotNull();
    }

    @Test
    @DisplayName("Listener container factory uses MANUAL_IMMEDIATE ack mode")
    void listenerContainerFactory_hasManualImmediateAckMode() {
        assertThat(detectionListenerContainerFactory.getContainerProperties().getAckMode())
                .isEqualTo(AckMode.MANUAL_IMMEDIATE);
    }

    // ─── Constants ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Topic constant matches expected Kafka topic name")
    void topicConstant_hasExpectedValue() {
        assertThat(DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS)
                .isEqualTo("normalised-ticks");
    }

    @Test
    @DisplayName("Consumer group constant has expected value")
    void consumerGroupConstant_hasExpectedValue() {
        assertThat(DetectionKafkaConsumerConfig.CONSUMER_GROUP)
                .isEqualTo("detection-engine-group");
    }
}
