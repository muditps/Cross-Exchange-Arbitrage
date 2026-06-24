package com.arbitrage.simulator;

import com.arbitrage.simulator.config.NormalisedTickConsumerConfig;
import com.arbitrage.simulator.config.SimulationKafkaConsumerConfig;
import com.arbitrage.simulator.config.SimulationProperties;
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
 * Verifies the Execution Simulator module wires up correctly.
 *
 * <p>Uses embedded Kafka to confirm the consumer bean is created with the correct
 * group ID, commit strategy, poll record limit, and ack mode. This is a
 * "context loads" test — it proves Spring assembles the module without errors
 * and all configuration values match the documented design decisions.
 *
 * <p><b>Why {@code @Import(KafkaAutoConfiguration.class)}?</b> Library module —
 * no {@code @SpringBootApplication} in src/main. {@code KafkaAutoConfiguration}
 * registers {@code KafkaProperties} which the consumer factory depends on.
 * Importing it explicitly gives us exactly what the factory needs without a full
 * Boot startup, matching the detection-engine test pattern (Session 3.1).
 *
 * <p><b>Why no {@code ExecutionSimulatorApplication.class}?</b> That class triggers a full
 * {@code @ComponentScan} of the simulator package. From Session 4.5 onwards, the scan picks up
 * {@code SimulationOrchestrator} → {@code SimulationResultRepository} → JPA EntityManagerFactory,
 * which is not available in this Kafka-only test context. This test loads only the two Kafka
 * config classes it actually tests, keeping the context minimal and fast.
 */
@SpringBootTest(classes = {
        ExecutionSimulatorApplicationTest.TestConfig.class,
        SimulationKafkaConsumerConfig.class,
        NormalisedTickConsumerConfig.class
})
@Import(KafkaAutoConfiguration.class)
@EmbeddedKafka(
        partitions = 3,
        topics = {
                SimulationKafkaConsumerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES,
                NormalisedTickConsumerConfig.TOPIC_NORMALISED_TICKS
        }
)
@ActiveProfiles("test")
class ExecutionSimulatorApplicationTest {

    /**
     * Provides beans not available in the minimal test context.
     * ObjectMapper is normally registered by JacksonAutoConfiguration — we provide
     * it here to keep the test focused, matching the detection-engine test approach.
     */
    @TestConfiguration
    static class TestConfig {
        @Bean
        public ObjectMapper objectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper;
        }

        @Bean
        public SimulationProperties simulationProperties() {
            return new SimulationProperties();
        }
    }

    @Autowired
    private ConsumerFactory<String, ?> simulationConsumerFactory;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, ?> simulationListenerContainerFactory;

    // ─── Consumer factory tests ───────────────────────────────────────────────

    @Test
    @DisplayName("Consumer factory uses execution-simulator-group")
    void consumerFactory_hasCorrectGroupId() {
        Map<String, Object> props = simulationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.GROUP_ID_CONFIG))
                .isEqualTo(SimulationKafkaConsumerConfig.CONSUMER_GROUP);
    }

    @Test
    @DisplayName("Consumer factory disables auto-commit for manual offset control")
    void consumerFactory_hasManualCommit() {
        Map<String, Object> props = simulationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
    }

    @Test
    @DisplayName("Consumer factory limits poll to 100 records to bound DB write time")
    void consumerFactory_hasMaxPollRecords() {
        Map<String, Object> props = simulationConsumerFactory.getConfigurationProperties();
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
    }

    // ─── Listener container factory tests ─────────────────────────────────────

    @Test
    @DisplayName("Listener container factory is created and non-null")
    void listenerContainerFactory_isCreated() {
        assertThat(simulationListenerContainerFactory).isNotNull();
    }

    @Test
    @DisplayName("Listener container factory uses MANUAL_IMMEDIATE ack mode")
    void listenerContainerFactory_hasManualImmediateAckMode() {
        assertThat(simulationListenerContainerFactory.getContainerProperties().getAckMode())
                .isEqualTo(AckMode.MANUAL_IMMEDIATE);
    }

    // ─── Constants ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Topic constant matches expected Kafka topic name")
    void topicConstant_hasExpectedValue() {
        assertThat(SimulationKafkaConsumerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES)
                .isEqualTo("arbitrage-opportunities");
    }

    @Test
    @DisplayName("Consumer group constant has expected value")
    void consumerGroupConstant_hasExpectedValue() {
        assertThat(SimulationKafkaConsumerConfig.CONSUMER_GROUP)
                .isEqualTo("execution-simulator-group");
    }
}
