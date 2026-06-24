package com.arbitrage.normalisation;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.normalisation.config.NormalisationKafkaConsumerConfig;
import com.arbitrage.normalisation.config.NormalisationKafkaProducerConfig;
import com.arbitrage.normalisation.service.FeedHealthMonitor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for the normalisation pipeline.
 *
 * <p>Verifies the complete message flow:
 * <pre>
 *   produce → raw-ticks-{binance|bybit|kucoin}
 *          → NormalisationService (@KafkaListener)
 *          → TickTransformerFactory routes to correct transformer
 *          → FeedHealthMonitor + ClockSkewMonitor updated
 *          → normalised-ticks topic
 *          → test consumer asserts results
 * </pre>
 *
 * <p><b>Why Testcontainers, not EmbeddedKafka?</b>
 * {@code @EmbeddedKafka} uses an in-process broker that is fast but behaviorally different
 * from a real Kafka broker — particularly around consumer group coordination, offset reset,
 * and partition assignment timing. Testcontainers runs a real Confluent Platform Kafka
 * container, catching integration issues that EmbeddedKafka would mask.
 * Session 2.3's {@link NormalisationEngineApplicationTest} uses EmbeddedKafka for fast
 * bean-wiring checks; this test uses Testcontainers for actual message flow.
 *
 * <p><b>Why a manual test consumer instead of another @KafkaListener?</b>
 * A {@code KafkaConsumer} gives us direct control over offset reset ({@code earliest}
 * per test) and a blocking poll loop with timeout. A {@code @KafkaListener} bean would
 * be shared across all tests and require extra synchronisation (latches, queues).
 *
 * <p><b>Topic creation:</b> {@link NewTopicConfig} registers {@link org.apache.kafka.clients.admin.NewTopic}
 * beans, which Spring Kafka's {@link KafkaAdmin} picks up and creates before any
 * listener container starts. This avoids the race between topic auto-creation and
 * consumer partition assignment.
 *
 * <p><b>Partition assignment wait:</b> The {@code @KafkaListener} container uses
 * {@code auto-offset-reset=latest}. If we produce before the container is assigned,
 * the consumer seeks to the new latest (after the message) and misses it.
 * {@link ContainerTestUtils#waitForAssignment} blocks until all 3 partitions are assigned
 * before any test produces messages.
 */
@SpringBootTest(classes = {
        NormalisationPipelineIntegrationTest.NewTopicConfig.class,
        NormalisationPipelineIntegrationTest.TestConfig.class,
        // Only @Configuration classes are safe to list directly in @SpringBootTest(classes).
        // Component beans (services, transformers) are discovered via the @ComponentScan
        // in TestConfig below, which targets only production sub-packages.
        // NormalisationEngineApplication is deliberately excluded — its root-level
        // @ComponentScan("com.arbitrage.normalisation") would scan the test classpath
        // and pick up other test @TestConfiguration classes, causing
        // BeanDefinitionOverrideException for shared bean names like 'objectMapper'.
        NormalisationKafkaConsumerConfig.class,
        NormalisationKafkaProducerConfig.class
})
@Import(KafkaAutoConfiguration.class)
@Testcontainers
@ActiveProfiles("test")
class NormalisationPipelineIntegrationTest {

    // ─── Kafka broker (one container shared for all tests in this class) ──────

    /**
     * Real Confluent Platform Kafka broker.
     * Static so the container is started once for the entire test class,
     * not restarted for each test method.
     */
    @Container
    static final KafkaContainer kafkaBroker =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    /**
     * Injects the Testcontainers broker address into the Spring environment
     * before the application context is initialised. This overrides any
     * {@code spring.kafka.bootstrap-servers} value from application.yml.
     */
    @DynamicPropertySource
    static void configureKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaBroker::getBootstrapServers);
    }

    // ─── Topic creation (runs during context init, before listeners start) ────

    /**
     * Registers {@link org.apache.kafka.clients.admin.NewTopic} beans that
     * {@link KafkaAdmin} creates on the broker during context initialisation.
     *
     * <p>Topics must exist before the {@code @KafkaListener} container starts
     * so that partition assignment succeeds immediately. Without this,
     * the listener retries assignment until Kafka auto-creates the topics,
     * causing a multi-second delay and flaky test behaviour.
     */
    @TestConfiguration
    static class NewTopicConfig {

        @Bean
        public org.apache.kafka.clients.admin.NewTopic rawTicksBinanceTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE, 1, (short) 1);
        }

        @Bean
        public org.apache.kafka.clients.admin.NewTopic rawTicksBybitTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT, 1, (short) 1);
        }

        @Bean
        public org.apache.kafka.clients.admin.NewTopic rawTicksKuCoinTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN, 1, (short) 1);
        }

        @Bean
        public org.apache.kafka.clients.admin.NewTopic normalisedTicksTopic() {
            return new org.apache.kafka.clients.admin.NewTopic(
                    NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS, 1, (short) 1);
        }
    }

    // ─── Test infrastructure beans ────────────────────────────────────────────

    /**
     * Provides infrastructure beans and triggers discovery of all production service beans.
     *
     * <p><b>Why targeted @ComponentScan instead of NormalisationEngineApplication?</b>
     * {@link NormalisationEngineApplication} uses {@code @ComponentScan("com.arbitrage.normalisation")},
     * which scans the root normalisation package — including the test classpath during test runs.
     * This causes {@link org.springframework.beans.factory.support.BeanDefinitionOverrideException}
     * because other test {@code @TestConfiguration} classes in the same package (e.g.,
     * {@code NormalisationEngineApplicationTest.TestConfig}) also define an {@code objectMapper} bean.
     * By scanning only the production sub-packages ({@code .service}, {@code .transformer}),
     * we get all the beans we need without touching the root package or test classes.</p>
     *
     * <p>{@code @EnableScheduling} is included here because it normally lives in
     * {@link NormalisationEngineApplication}, which is excluded from this test context.</p>
     */
    @TestConfiguration
    @EnableScheduling
    @ComponentScan(basePackages = {
            "com.arbitrage.normalisation.service",
            "com.arbitrage.normalisation.transformer"
    })
    static class TestConfig {

        @Bean
        public ObjectMapper objectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper;
        }

        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    // ─── Injected beans ───────────────────────────────────────────────────────

    /**
     * Reused as the test producer for raw-ticks-* topics.
     * The raw ticks format is {@link NormalisedTick} (same type as normalised),
     * so this template's JSON serialization works for both raw and normalised topics.
     */
    @Autowired
    @Qualifier("normalisedTickKafkaTemplate")
    private KafkaTemplate<String, NormalisedTick> rawTickProducer;

    /**
     * Used to wait for the {@code @KafkaListener} container to complete partition
     * assignment before each test produces messages.
     */
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    /** Asserted after tick processing to verify feed health state transitions. */
    @Autowired
    private FeedHealthMonitor feedHealthMonitor;

    // ─── Per-test consumer ────────────────────────────────────────────────────

    /**
     * A fresh KafkaConsumer per test, with a unique group ID.
     * Subscribes to {@code normalised-ticks} with {@code auto.offset.reset=latest}:
     * the initial poll in {@link #setUp()} advances to the current latest offset,
     * so each test only sees records produced during that specific test run.
     */
    private KafkaConsumer<String, NormalisedTick> normalisedTickConsumer;

    @BeforeEach
    void setUp() throws InterruptedException {
        // Wait for the @KafkaListener container to be assigned all 3 partitions
        // (1 partition each for raw-ticks-binance, raw-ticks-bybit, raw-ticks-kucoin).
        // This MUST complete before producing, because auto-offset-reset=latest:
        // if we produce before assignment, the consumer seeks past the message and misses it.
        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 3);
        }

        // Create a fresh consumer per test — unique group ID prevents cross-test offset sharing.
        // latest reset + initial empty poll = consumer starts at the current end of the topic,
        // so only records produced in THIS test run are visible.
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        JsonDeserializer<NormalisedTick> valueDeserializer =
                new JsonDeserializer<>(NormalisedTick.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");

        normalisedTickConsumer = new KafkaConsumer<>(
                consumerProps, new StringDeserializer(), valueDeserializer);
        normalisedTickConsumer.subscribe(
                List.of(NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS));

        // Trigger partition assignment and advance to the latest offset.
        // This empty poll blocks until the consumer is assigned its partition,
        // then returns 0 records (nothing new yet). Subsequent polls will
        // return only records produced after this point.
        normalisedTickConsumer.poll(Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() {
        if (normalisedTickConsumer != null) {
            normalisedTickConsumer.close();
        }
    }

    // ─── Tests ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Binance raw tick flows through pipeline and appears in normalised-ticks")
    void binanceTick_flowsThroughPipeline_appearsInNormalisedTicks() {
        NormalisedTick rawTick = buildRawTick(ExchangeId.BINANCE, "67250.50000000", "67251.30000000");

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));

        assertThat(record)
                .as("A normalised tick should appear in normalised-ticks within 15 seconds")
                .isNotNull();
        assertThat(record.value().getExchangeId()).isEqualTo(ExchangeId.BINANCE);
        assertThat(record.value().getBestBidPrice())
                .isEqualByComparingTo(new BigDecimal("67250.50000000"));
        assertThat(record.value().getBestAskPrice())
                .isEqualByComparingTo(new BigDecimal("67251.30000000"));
    }

    @Test
    @DisplayName("Bybit raw tick flows through pipeline and appears in normalised-ticks")
    void bybitTick_flowsThroughPipeline_appearsInNormalisedTicks() {
        NormalisedTick rawTick = buildRawTick(ExchangeId.BYBIT, "67300.10000000", "67301.00000000");

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));

        assertThat(record).isNotNull();
        assertThat(record.value().getExchangeId()).isEqualTo(ExchangeId.BYBIT);
        assertThat(record.value().getBestBidPrice())
                .isEqualByComparingTo(new BigDecimal("67300.10000000"));
    }

    @Test
    @DisplayName("KuCoin raw tick flows through pipeline and appears in normalised-ticks")
    void kuCoinTick_flowsThroughPipeline_appearsInNormalisedTicks() {
        NormalisedTick rawTick = buildRawTick(ExchangeId.KUCOIN, "67280.00000000", "67282.50000000");

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));

        assertThat(record).isNotNull();
        assertThat(record.value().getExchangeId()).isEqualTo(ExchangeId.KUCOIN);
        assertThat(record.value().getBestBidPrice())
                .isEqualByComparingTo(new BigDecimal("67280.00000000"));
    }

    @Test
    @DisplayName("All three exchanges: one raw tick each → all three appear in normalised-ticks")
    void allThreeExchanges_oneRawTickEach_allAppearInNormalisedTicks() {
        NormalisedTick binanceTick = buildRawTick(ExchangeId.BINANCE, "67250.00", "67251.00");
        NormalisedTick bybitTick   = buildRawTick(ExchangeId.BYBIT,   "67300.00", "67301.00");
        NormalisedTick kuCoinTick  = buildRawTick(ExchangeId.KUCOIN,  "67280.00", "67282.00");

        rawTickProducer.send(NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                binanceTick.getTradingPair().canonicalSymbol(), binanceTick);
        rawTickProducer.send(NormalisationKafkaConsumerConfig.TOPIC_RAW_BYBIT,
                bybitTick.getTradingPair().canonicalSymbol(), bybitTick);
        rawTickProducer.send(NormalisationKafkaConsumerConfig.TOPIC_RAW_KUCOIN,
                kuCoinTick.getTradingPair().canonicalSymbol(), kuCoinTick);

        List<ConsumerRecord<String, NormalisedTick>> records = pollRecords(3, Duration.ofSeconds(20));

        Set<ExchangeId> receivedExchanges = new HashSet<>();
        for (ConsumerRecord<String, NormalisedTick> record : records) {
            receivedExchanges.add(record.value().getExchangeId());
        }

        assertThat(receivedExchanges)
                .as("All three exchanges must produce a normalised tick — pipeline routes all three correctly")
                .containsExactlyInAnyOrder(ExchangeId.BINANCE, ExchangeId.BYBIT, ExchangeId.KUCOIN);
    }

    @Test
    @DisplayName("Normalised tick message key is 'EXCHANGE:PAIR' for balanced partition distribution")
    void normalisedTick_messageKey_isExchangeColonPair() {
        // KEY CONCEPT: The key uses "EXCHANGE:PAIR" (e.g. "BINANCE:BTC-USDT") to distribute
        // the 9 independent producer streams (3 exchanges × 3 pairs) evenly across partitions.
        // The detection engine stores per-exchange prices in Redis, so same-partition co-location
        // of different exchanges for the same pair is not required for correctness.
        NormalisedTick rawTick = buildRawTick(ExchangeId.BINANCE, "50000.00000000", "50001.00000000");

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));

        assertThat(record).isNotNull();
        assertThat(record.key())
                .as("Message key must be 'EXCHANGE:PAIR' for even partition distribution")
                .isEqualTo("BINANCE:BTC-USDT");
    }

    @Test
    @DisplayName("processedTimestamp (T4) is strictly greater than receivedTimestamp (T0)")
    void normalisedTick_processedTimestamp_isStrictlyAfterReceivedTimestamp() {
        // T0 is captured by the exchange connector at WebSocket message arrival.
        // T4 is stamped by the transformer after all field validation.
        // This test proves the timestamp chain is intact through serialisation → Kafka → deserialisation.
        NormalisedTick rawTick = buildRawTick(ExchangeId.BINANCE, "50000.00000000", "50001.00000000");
        final long originalReceivedTimestamp = rawTick.getReceivedTimestamp();

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));

        assertThat(record).isNotNull();
        assertThat(record.value().getReceivedTimestamp())
                .as("T0 (receivedTimestamp) must be preserved unchanged through normalisation")
                .isEqualTo(originalReceivedTimestamp);
        assertThat(record.value().getProcessedTimestamp())
                .as("T4 (processedTimestamp) must be strictly after T0 (set by transformer)")
                .isGreaterThan(originalReceivedTimestamp);
    }

    @Test
    @DisplayName("FeedHealthMonitor transitions to CONNECTED after a tick is processed")
    void feedHealthMonitor_afterTickProcessed_statusIsConnected() {
        // FeedHealthMonitor.recordTickReceived() is called inside NormalisationService
        // after successful transformation. Polling normalised-ticks confirms processing
        // has completed, so the status assertion is safe (no race condition).
        NormalisedTick rawTick = buildRawTick(ExchangeId.BINANCE, "67000.00000000", "67001.00000000");

        rawTickProducer.send(
                NormalisationKafkaConsumerConfig.TOPIC_RAW_BINANCE,
                rawTick.getTradingPair().canonicalSymbol(),
                rawTick);

        // Wait for processing to complete — normalised tick in output topic = processing done
        ConsumerRecord<String, NormalisedTick> record = pollSingleRecord(Duration.ofSeconds(15));
        assertThat(record)
                .as("Tick must appear in normalised-ticks before checking feed health status")
                .isNotNull();

        assertThat(feedHealthMonitor.getStatus(ExchangeId.BINANCE))
                .as("FeedHealthMonitor must show CONNECTED after successfully processing a Binance tick")
                .isEqualTo(FeedStatus.CONNECTED);
    }

    // ─── Poll helpers ─────────────────────────────────────────────────────────

    /**
     * Polls {@code normalised-ticks} for a single record, waiting up to {@code timeout}.
     *
     * @param timeout maximum wait duration
     * @return the first record received, or {@code null} if none arrived within the timeout
     */
    private ConsumerRecord<String, NormalisedTick> pollSingleRecord(Duration timeout) {
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<String, NormalisedTick> records =
                    normalisedTickConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, NormalisedTick> record : records) {
                return record;
            }
        }
        return null;
    }

    /**
     * Polls {@code normalised-ticks} until {@code expectedCount} records are received
     * or the timeout is reached.
     *
     * @param expectedCount target number of records
     * @param timeout       maximum wait duration
     * @return all records received; may be fewer than {@code expectedCount} on timeout
     */
    private List<ConsumerRecord<String, NormalisedTick>> pollRecords(int expectedCount, Duration timeout) {
        List<ConsumerRecord<String, NormalisedTick>> collected = new ArrayList<>();
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (collected.size() < expectedCount && System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<String, NormalisedTick> batch =
                    normalisedTickConsumer.poll(Duration.ofMillis(500));
            batch.forEach(collected::add);
        }
        return collected;
    }

    // ─── Test data ────────────────────────────────────────────────────────────

    /**
     * Builds a raw tick representing what the exchange connector would publish to Kafka.
     * All required fields are populated so transformers accept the tick without dropping it.
     *
     * @param exchangeId the exchange this tick originates from
     * @param bidPrice   best bid price as a string (BigDecimal-safe)
     * @param askPrice   best ask price as a string (BigDecimal-safe)
     * @return a fully populated NormalisedTick ready for the raw-ticks topic
     */
    private static NormalisedTick buildRawTick(
            ExchangeId exchangeId, String bidPrice, String askPrice) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(TradingPair.fromSymbol("BTC-USDT"))
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(askPrice))
                .bestBidQuantity(new BigDecimal("1.50000000"))
                .bestAskQuantity(new BigDecimal("2.00000000"))
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }
}
