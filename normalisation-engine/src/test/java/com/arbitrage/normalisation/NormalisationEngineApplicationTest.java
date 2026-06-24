package com.arbitrage.normalisation;

import com.arbitrage.normalisation.config.NormalisationKafkaConsumerConfig;
import com.arbitrage.normalisation.config.NormalisationKafkaProducerConfig;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the Normalisation Engine module wires up correctly.
 *
 * <p>Uses embedded Kafka (no Docker required) to test that consumer and producer
 * beans are created with the expected configuration. This is a "context loads" test —
 * it proves the Spring context assembles without errors and beans have the
 * right properties set.
 *
 * <p><b>Why {@code @Import(KafkaAutoConfiguration.class)}?</b> This is a library
 * module — it has no {@code @SpringBootApplication} class in src/main. The Kafka
 * config beans depend on {@link org.springframework.boot.autoconfigure.kafka.KafkaProperties},
 * which is registered by {@code KafkaAutoConfiguration}. Importing it explicitly
 * avoids a full Spring Boot startup while providing exactly what the beans need.
 *
 * <p><b>Why embedded Kafka?</b> Consumer and producer factories validate
 * bootstrap-servers at context creation. {@code @EmbeddedKafka} provides an
 * in-process broker in milliseconds — no Docker, no Testcontainers overhead.
 */
@SpringBootTest(classes = {
        NormalisationEngineApplicationTest.TestConfig.class,
        NormalisationKafkaConsumerConfig.class,
        NormalisationKafkaProducerConfig.class
})
@Import(KafkaAutoConfiguration.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT,
                NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN,
                NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS
        }
)
@ActiveProfiles("test")
class NormalisationEngineApplicationTest {

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
    private ConsumerFactory<String, ?> normalisationConsumerFactory;

    @Autowired
    private ProducerFactory<String, ?> normalisedTickProducerFactory;

    @Autowired
    private KafkaTemplate<String, ?> normalisedTickKafkaTemplate;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, ?> normalisationListenerContainerFactory;

    // ─── Consumer factory tests ───────────────────────────────────────────────

    @Test
    @DisplayName("Consumer factory uses normalisation-engine-group")
    void consumerFactory_hasCorrectGroupId() {
        Map<String, Object> props = normalisationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG))
                .isEqualTo(NormalisationKafkaConsumerConfig.CONSUMER_GROUP);
    }

    @Test
    @DisplayName("Consumer factory disables auto-commit for manual offset control")
    void consumerFactory_hasManualCommit() {
        Map<String, Object> props = normalisationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
    }

    @Test
    @DisplayName("Consumer factory sets max-poll-records to 500")
    void consumerFactory_hasMaxPollRecords() {
        Map<String, Object> props = normalisationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(500);
    }

    // ─── Listener container factory tests ─────────────────────────────────────

    @Test
    @DisplayName("Listener container factory is created and non-null")
    void listenerContainerFactory_isCreated() {
        assertThat(normalisationListenerContainerFactory).isNotNull();
    }

    @Test
    @DisplayName("Listener container factory uses MANUAL_IMMEDIATE ack mode")
    void listenerContainerFactory_hasManualImmediateAckMode() {
        assertThat(normalisationListenerContainerFactory.getContainerProperties().getAckMode())
                .isEqualTo(AckMode.MANUAL_IMMEDIATE);
    }

    // ─── Producer / template tests ────────────────────────────────────────────

    @Test
    @DisplayName("Normalised tick KafkaTemplate is created and non-null")
    void normalisedTickKafkaTemplate_isCreated() {
        assertThat(normalisedTickKafkaTemplate).isNotNull();
    }

    // ─── Constants ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Topic constants match expected Kafka topic names")
    void topicConstants_haveExpectedValues() {
        assertThat(NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE).isEqualTo("raw-ticks-binance");
        assertThat(NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT).isEqualTo("raw-ticks-bybit");
        assertThat(NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN).isEqualTo("raw-ticks-kucoin");
        assertThat(NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS).isEqualTo("normalised-ticks");
    }

    @Test
    @DisplayName("Consumer group constant has expected value")
    void consumerGroupConstant_hasExpectedValue() {
        assertThat(NormalisationKafkaConsumerConfig.CONSUMER_GROUP)
                .isEqualTo("normalisation-engine-group");
    }
}
