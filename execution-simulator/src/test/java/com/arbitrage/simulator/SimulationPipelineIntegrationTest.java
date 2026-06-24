package com.arbitrage.simulator;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.simulator.config.LatencyConfiguration;
import com.arbitrage.simulator.config.NormalisedTickConsumerConfig;
import com.arbitrage.simulator.config.SimulationKafkaConsumerConfig;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.entity.SimulationResult;
import com.arbitrage.simulator.repository.SimulationResultRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test for the simulation pipeline.
 *
 * <p>Proves that a {@link com.arbitrage.common.model.OpportunityStatus#CLOSED} opportunity
 * consumed from Kafka is fully processed by {@link com.arbitrage.simulator.listener.SimulationOrchestrator}
 * and persisted as a {@link SimulationResult} row in PostgreSQL.
 *
 * <p>Uses plain {@code postgres:16-alpine} (no TimescaleDB extension needed) with a
 * test-specific Flyway migration at {@code db/migration/V1__test_schema.sql} that creates
 * the {@code simulation_results} table without {@code create_hypertable()} calls.
 *
 * <p>No {@code @ActiveProfiles("test")} — that profile excludes JPA autoconfiguration.
 * All properties are provided via {@link DynamicPropertySource}.
 */
@SpringBootTest(classes = {
        SimulationPipelineIntegrationTest.TopicConfig.class,
        SimulationPipelineIntegrationTest.TestConfig.class,
        SimulationKafkaConsumerConfig.class,
        NormalisedTickConsumerConfig.class
})
@ImportAutoConfiguration({
        KafkaAutoConfiguration.class,
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        TransactionAutoConfiguration.class,
        FlywayAutoConfiguration.class
})
@Testcontainers
class SimulationPipelineIntegrationTest {

    @Container
    static final KafkaContainer kafkaBroker =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @SuppressWarnings("resource")
    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaBroker::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("arbitrage.simulation.enabled", () -> "true");
        registry.add("spring.kafka.listener.missing-topics-fatal", () -> "false");
        // ExecutionSimulatorApplicationTest$TestConfig (a @TestConfiguration in the same package)
        // is picked up by @ComponentScan and also defines objectMapper. Allowing override lets
        // this test's own TestConfig take precedence without a BeanDefinitionOverrideException.
        registry.add("spring.main.allow-bean-definition-overriding", () -> "true");
    }

    // ── Topic beans ───────────────────────────────────────────────────────────

    @TestConfiguration
    static class TopicConfig {
        @Bean
        org.apache.kafka.clients.admin.NewTopic opportunitiesTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    SimulationKafkaConsumerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES, 1, (short) 1);
        }

        @Bean
        org.apache.kafka.clients.admin.NewTopic normalisedTicksTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    NormalisedTickConsumerConfig.TOPIC_NORMALISED_TICKS, 1, (short) 1);
        }
    }

    // ── Application beans + component scan ───────────────────────────────────

    @TestConfiguration
    @Import({
            com.arbitrage.simulator.listener.SimulationOrchestrator.class,
            com.arbitrage.simulator.listener.NormalisedTickListener.class,
            com.arbitrage.simulator.service.ExecutionTimelineSimulator.class,
            com.arbitrage.simulator.service.SlippageEstimator.class,
            com.arbitrage.simulator.service.HistoricalPriceStore.class,
            com.arbitrage.simulator.service.PriceAtExecutionLookup.class
    })
    @EnableConfigurationProperties({SimulationProperties.class, LatencyConfiguration.class})
    @EntityScan(basePackages = "com.arbitrage.simulator.entity")
    @EnableJpaRepositories(basePackages = "com.arbitrage.simulator.repository")
    @EnableScheduling
    static class TestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().registerModule(new JavaTimeModule());
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean("opportunityTestProducer")
        KafkaTemplate<String, ArbitrageOpportunity> opportunityTestProducer(
                KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
            Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            DefaultKafkaProducerFactory<String, ArbitrageOpportunity> factory =
                    new DefaultKafkaProducerFactory<>(props);
            factory.setValueSerializer(new JsonSerializer<>(objectMapper));
            return new KafkaTemplate<>(factory);
        }
    }

    // ── Injected beans ────────────────────────────────────────────────────────

    @Autowired
    @Qualifier("opportunityTestProducer")
    private KafkaTemplate<String, ArbitrageOpportunity> producer;

    @Autowired
    private SimulationResultRepository repository;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @BeforeEach
    void waitForListenerAssignment() throws InterruptedException {
        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }
    }

    @AfterEach
    void cleanDatabase() {
        repository.deleteAll();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("CLOSED opportunity is simulated and persisted with correct fields")
    void closedOpportunity_simulationRowWrittenWithCorrectFields() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50200");

        produce(opp);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SimulationResult> rows = repository.findAll();
            assertThat(rows).hasSize(1);

            SimulationResult row = rows.get(0);
            assertThat(row.getId()).isNotNull();
            assertThat(row.getOpportunityId()).isEqualTo(opp.getId());
            assertThat(row.getSimulatedLatencyMs()).isGreaterThan(0L);
            assertThat(row.getSimulatedQuantity()).isEqualByComparingTo("1");
            assertThat(row.getFillProbability()).isEqualByComparingTo("1.0000");
            assertThat(row.getSimulationTimestamp()).isNotNull();
            // Price store is empty on startup → slippage estimator uses fallback (2 bps per leg)
            assertThat(row.getSlippageBps()).isEqualByComparingTo("4.00");
        });
    }

    @Test
    @DisplayName("DETECTED opportunity is skipped — no row written to database")
    void detectedOpportunity_noRowWritten() throws InterruptedException {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.DETECTED, "50000", "50200");

        produce(opp);

        // Wait long enough for the consumer to have processed the message if it were going to
        Thread.sleep(5_000);
        assertThat(repository.findAll()).isEmpty();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ArbitrageOpportunity buildOpportunity(OpportunityStatus status,
                                                   String buyPrice, String sellPrice) {
        BigDecimal buy  = new BigDecimal(buyPrice);
        BigDecimal sell = new BigDecimal(sellPrice);
        BigDecimal qty  = BigDecimal.ONE;
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(TradingPair.fromSymbol("BTC-USDT"))
                .buyExchange(ExchangeId.BINANCE)
                .sellExchange(ExchangeId.BYBIT)
                .buyPrice(buy)
                .buyQuantity(qty)
                .buyFeeRate(new BigDecimal("0.0010"))
                .sellPrice(sell)
                .sellQuantity(qty)
                .sellFeeRate(new BigDecimal("0.0010"))
                .grossSpread(sell.subtract(buy))
                .netSpread(sell.subtract(buy))
                .grossSpreadBps(BigDecimal.TEN)
                .netSpreadBps(BigDecimal.ONE)
                .arbitrageableQuantity(qty)
                .theoreticalProfit(sell.subtract(buy))
                .status(status)
                .detectionTimestamp(Instant.now().minusMillis(200))
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(System.nanoTime())
                .closedNanoTime(System.nanoTime())
                .peakNetSpread(sell.subtract(buy))
                .averageNetSpread(sell.subtract(buy))
                .totalDurationMs(100L)
                .updateCount(3L)
                .build();
    }

    private void produce(ArbitrageOpportunity opportunity) {
        producer.send(SimulationKafkaConsumerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES,
                opportunity.getId().toString(), opportunity);
    }
}
