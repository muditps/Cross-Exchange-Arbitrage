package com.arbitrage.detection;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.arbitrage.detection.config.DetectionKafkaConsumerConfig;
import com.arbitrage.detection.config.DetectionKafkaProducerConfig;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.config.DetectionRedisConfig;
import com.arbitrage.detection.config.FeeConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {
        DetectionPipelineIntegrationTest.NewTopicConfig.class,
        DetectionPipelineIntegrationTest.TestConfig.class,
        DetectionKafkaConsumerConfig.class,
        DetectionKafkaProducerConfig.class,
        DetectionRedisConfig.class
})
@Import({
        KafkaAutoConfiguration.class,
        org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class,
        org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration.class
})
@Testcontainers
@ActiveProfiles("test")
class DetectionPipelineIntegrationTest {

    @Container
    static final KafkaContainer kafkaBroker =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> redisContainer =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaBroker::getBootstrapServers);
        registry.add("spring.data.redis.host", redisContainer::getHost);
        registry.add("spring.data.redis.port", () -> redisContainer.getMappedPort(6379));
        // Extend staleness threshold to 30 s so normal test ticks are never stale
        registry.add("arbitrage.detection.staleness-threshold-ms", () -> 30_000);
        registry.add("arbitrage.detection.redis-price-ttl-ms", () -> 30_000);
    }

    // ── Topic creation ────────────────────────────────────────────────────────

    @TestConfiguration
    static class NewTopicConfig {
        @Bean
        org.apache.kafka.clients.admin.NewTopic normalisedTicksTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS, 1, (short) 1);
        }

        @Bean
        org.apache.kafka.clients.admin.NewTopic arbitrageOpportunitiesTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    DetectionKafkaProducerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES, 1, (short) 1);
        }
    }

    // ── Infrastructure beans + service scan ───────────────────────────────────

    @TestConfiguration
    @EnableScheduling
    @EnableConfigurationProperties({DetectionProperties.class, FeeConfiguration.class})
    @ComponentScan(basePackages = "com.arbitrage.detection.service")
    static class TestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper().registerModule(new JavaTimeModule());
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean("normalisedTickTestProducer")
        KafkaTemplate<String, NormalisedTick> normalisedTickTestProducer(
                KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
            Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            DefaultKafkaProducerFactory<String, NormalisedTick> factory =
                    new DefaultKafkaProducerFactory<>(props);
            factory.setValueSerializer(new JsonSerializer<>(objectMapper));
            return new KafkaTemplate<>(factory);
        }
    }

    // ── Injected fields ───────────────────────────────────────────────────────

    @Autowired
    @Qualifier("normalisedTickTestProducer")
    private KafkaTemplate<String, NormalisedTick> tickProducer;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    private KafkaConsumer<String, ArbitrageOpportunity> opportunityConsumer;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @BeforeEach
    void setUp() throws InterruptedException {
        // Block until the @KafkaListener container is assigned its partition.
        // auto-offset-reset=latest means messages produced before assignment are missed.
        for (MessageListenerContainer container : listenerRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 1);
        }

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        // useHeadersIfPresent=false: always decode to ArbitrageOpportunity, ignore __TypeId__ headers
        // the producer's JsonSerializer adds type headers; ignoring them avoids ClassNotFound errors
        JsonDeserializer<ArbitrageOpportunity> valueDeser =
                new JsonDeserializer<>(ArbitrageOpportunity.class, mapper, false);

        opportunityConsumer = new KafkaConsumer<>(props, new StringDeserializer(), valueDeser);
        opportunityConsumer.subscribe(
                List.of(DetectionKafkaProducerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES));
        // Advance to latest — only records produced in THIS test are visible after this point
        opportunityConsumer.poll(Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() {
        if (opportunityConsumer != null) {
            opportunityConsumer.close();
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Profitable spread: Binance bid > Bybit ask after fees → DETECTED event published")
    void profitableSpread_emitsDetectedEvent() {
        TradingPair pair = TradingPair.fromSymbol("BTC-USDT");

        // Binance bid=51000, Bybit ask=50001 → gross spread 999 USDT
        // Fees: 50001 * 0.001 + 51000 * 0.001 = 101.001 → net spread 897.999 USDT (positive)
        produce(TestTickGenerator.freshTick(ExchangeId.BYBIT,   pair, "50000", "50001"));
        produce(TestTickGenerator.freshTick(ExchangeId.BINANCE, pair, "51000", "51001"));

        ConsumerRecord<String, ArbitrageOpportunity> record = pollOne(Duration.ofSeconds(20));

        assertThat(record)
                .as("A DETECTED event must be published for a net-positive spread")
                .isNotNull();
        ArbitrageOpportunity opp = record.value();
        assertThat(opp.getStatus()).isEqualTo(OpportunityStatus.DETECTED);
        assertThat(opp.getBuyExchange()).isEqualTo(ExchangeId.BYBIT);
        assertThat(opp.getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(opp.getTradingPair().canonicalSymbol()).isEqualTo("BTC-USDT");
        assertThat(opp.getNetSpread())
                .as("Net spread must be positive after fees")
                .isPositive();
    }

    @Test
    @DisplayName("Lifecycle: profitable spread opens then collapses → DETECTED then CLOSED events")
    void lifecycle_detectedThenClosed_bothEventsEmitted() {
        TradingPair pair = TradingPair.fromSymbol("BNB-USDT");

        // Step 1: seed Bybit price (only 1 exchange → no comparison yet)
        produce(TestTickGenerator.freshTick(ExchangeId.BYBIT,   pair, "50000", "50001"));

        // Step 2: Binance tick triggers comparison → DETECTED
        // Sell Binance(51000) / Buy Bybit(50001): net spread ~898 USDT (profitable)
        produce(TestTickGenerator.freshTick(ExchangeId.BINANCE, pair, "51000", "51001"));

        // Step 3: Bybit ask rises above Binance bid → spread collapses → CLOSED
        // Sell Binance(51000)/Buy Bybit(51100): gross=-100 (fails)
        // Sell Bybit(51000)/Buy Binance(51001): gross=-1 (fails) → both directions die
        produce(TestTickGenerator.freshTick(ExchangeId.BYBIT,   pair, "51000", "51100"));

        List<ConsumerRecord<String, ArbitrageOpportunity>> records = pollN(2, Duration.ofSeconds(20));

        assertThat(records).as("Both DETECTED and CLOSED events must be published").hasSize(2);

        ArbitrageOpportunity detected = records.get(0).value();
        assertThat(detected.getStatus()).isEqualTo(OpportunityStatus.DETECTED);
        assertThat(detected.getBuyExchange()).isEqualTo(ExchangeId.BYBIT);
        assertThat(detected.getSellExchange()).isEqualTo(ExchangeId.BINANCE);

        ArbitrageOpportunity closed = records.get(1).value();
        assertThat(closed.getStatus()).isEqualTo(OpportunityStatus.CLOSED);
        assertThat(closed.getBuyExchange()).isEqualTo(ExchangeId.BYBIT);
        assertThat(closed.getSellExchange()).isEqualTo(ExchangeId.BINANCE);
        assertThat(closed.getTotalDurationMs()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("Stale price: receivedTimestamp 60 s old exceeds 30 s threshold → no event published")
    void stalePrice_noEventEmitted() {
        TradingPair pair = TradingPair.fromSymbol("SOL-USDT");

        // Fresh Binance tick stored first (1 exchange only → no comparison yet)
        produce(TestTickGenerator.freshTick(ExchangeId.BINANCE, pair, "51000", "51001"));

        // Bybit tick is 60 s old — staleness threshold in test is 30 s → rejected by StalenessFilter
        // Prices would be profitable if fresh (same math as Test 1), proving the filter is the cause
        produce(TestTickGenerator.staleTick(ExchangeId.BYBIT, pair, "50000", "50001", 60));

        ConsumerRecord<String, ArbitrageOpportunity> record = pollOne(Duration.ofSeconds(5));

        assertThat(record)
                .as("No event must be published when the buy-side price is stale")
                .isNull();
    }

    @Test
    @DisplayName("Net-negative spread: fees exceed gross profit → no event published")
    void netNegativeSpread_noEventEmitted() {
        TradingPair pair = TradingPair.fromSymbol("ETH-USDT");

        // Binance bid=50080, Bybit ask=50001 → gross spread 79 USDT (~15.8 bps, passes noise filter)
        // Fees: 50001*0.001 + 50080*0.001 = 100.081 → net spread -21.081 (negative, filtered)
        produce(TestTickGenerator.freshTick(ExchangeId.BYBIT,   pair, "50000", "50001"));
        produce(TestTickGenerator.freshTick(ExchangeId.BINANCE, pair, "50080", "50081"));

        ConsumerRecord<String, ArbitrageOpportunity> record = pollOne(Duration.ofSeconds(5));

        assertThat(record)
                .as("No event must be published when fees eliminate the gross profit")
                .isNull();
    }

    // ── Poll helpers ──────────────────────────────────────────────────────────

    private ConsumerRecord<String, ArbitrageOpportunity> pollOne(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, ArbitrageOpportunity> batch =
                    opportunityConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, ArbitrageOpportunity> r : batch) {
                return r;
            }
        }
        return null;
    }

    private List<ConsumerRecord<String, ArbitrageOpportunity>> pollN(int n, Duration timeout) {
        List<ConsumerRecord<String, ArbitrageOpportunity>> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (collected.size() < n && System.currentTimeMillis() < deadline) {
            opportunityConsumer.poll(Duration.ofMillis(500)).forEach(collected::add);
        }
        return collected;
    }

    private void produce(NormalisedTick tick) {
        tickProducer.send(
                DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS,
                tick.getTradingPair().canonicalSymbol(),
                tick);
    }
}
