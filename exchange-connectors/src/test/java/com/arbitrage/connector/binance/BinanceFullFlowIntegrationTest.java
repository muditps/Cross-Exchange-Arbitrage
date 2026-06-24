package com.arbitrage.connector.binance;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.arbitrage.connector.testutil.TestWebSocketServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full end-to-end integration test: WebSocket → Connector → Parser → Kafka Producer → Real Kafka.
 *
 * <p><b>What this proves:</b> Unit tests verify components in isolation with mocks.
 * This test proves the entire pipeline works with real infrastructure:</p>
 * <ul>
 *   <li>A local WebSocket server sends recorded Binance fixtures</li>
 *   <li>BinanceConnector receives and delegates to BinanceMessageParser</li>
 *   <li>BinanceTickKafkaProducer publishes each NormalisedTick to Kafka</li>
 *   <li>A real Kafka broker (Testcontainers) stores the messages</li>
 *   <li>A KafkaConsumer reads the messages and verifies the tick values</li>
 * </ul>
 *
 * <p><b>KEY CONCEPT — Testcontainers:</b> Spins up a real Kafka broker in a Docker
 * container for the duration of the test. The test talks to a real broker, not a mock.
 * When mocks disagree with reality (serialization format, partition assignment, consumer
 * group behaviour), Testcontainers catches it. This is the gold standard for integration
 * testing — the same approach used at trading firms and major tech companies.</p>
 *
 * <p><b>Why not @SpringBootTest?</b> We wire components manually to keep the test fast
 * (~3-5 seconds) and focused. @SpringBootTest would scan the entire classpath, start
 * unnecessary beans, and take ~15 seconds. Manual wiring also makes dependencies explicit
 * — you can see exactly what the pipeline needs.</p>
 *
 * <p><b>Requires Docker:</b> Testcontainers needs a running Docker daemon. If Docker is
 * not available, the test is skipped (Testcontainers throws an IllegalStateException).</p>
 */
@Testcontainers
class BinanceFullFlowIntegrationTest {

    private static final String SUBSCRIPTION_ACK = "{\"result\":null,\"id\":1}";
    private static final String BOOK_TICKER_1 =
            "{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"67250.50000000\",\"B\":\"1.23400000\",\"a\":\"67251.30000000\",\"A\":\"0.98700000\"}";
    private static final String BOOK_TICKER_2 =
            "{\"u\":400900218,\"s\":\"BTCUSDT\",\"b\":\"67255.10000000\",\"B\":\"2.50000000\",\"a\":\"67256.00000000\",\"A\":\"1.10000000\"}";

    /**
     * Real Kafka broker running in Docker via Testcontainers.
     *
     * <p>{@code @Container} tells Testcontainers to start this container before
     * tests and stop it after. The Confluent Kafka image includes both the broker
     * and a built-in KRaft controller (no Zookeeper needed).</p>
     *
     * <p>{@code static} makes the container shared across all test methods in this
     * class — starting a Kafka container takes ~3 seconds, so reusing it across
     * tests saves significant time.</p>
     */
    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private TestWebSocketServer webSocketServer;
    private BinanceConnector connector;
    private BinanceTickKafkaProducer kafkaProducer;
    private KafkaConsumer<String, NormalisedTick> consumer;
    private TradingPair btcUsdt;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        // Jackson ObjectMapper with JavaTimeModule for Instant serialization
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // WebSocket server that simulates Binance
        webSocketServer = new TestWebSocketServer(
                List.of(SUBSCRIPTION_ACK, BOOK_TICKER_1, BOOK_TICKER_2),
                Duration.ofMillis(50));
        webSocketServer.start();

        // Connector config pointing to our local WebSocket server
        ExchangeConnectorProperties.ExchangeProperties config =
                new ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint(webSocketServer.getWsUri());
        config.setTakerFeeRate(new BigDecimal("0.0010"));
        config.setInitialReconnectDelay(Duration.ofSeconds(1));
        config.setMaxReconnectDelay(Duration.ofSeconds(5));
        config.setMaxReconnectAttempts(3);
        config.setStalenessThreshold(Duration.ofSeconds(5));
        config.setEnabled(true);

        // Wire up the pipeline manually: connector → parser → Kafka producer
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        BinanceMessageParser parser = new BinanceMessageParser(objectMapper, metrics);
        connector = new BinanceConnector(config, parser,
                new ReactorNettyWebSocketClient(), metrics);

        // Kafka producer factory pointing to the Testcontainers Kafka broker
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0); // send immediately in tests
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);

        DefaultKafkaProducerFactory<String, NormalisedTick> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        producerFactory.setValueSerializer(new JsonSerializer<>(objectMapper));

        KafkaTemplate<String, NormalisedTick> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        kafkaProducer = new BinanceTickKafkaProducer(connector, kafkaTemplate, true);

        // Kafka consumer to verify messages landed in the topic
        consumer = createTestConsumer();

        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    @AfterEach
    void tearDown() {
        if (kafkaProducer != null) {
            kafkaProducer.stop();
        }
        if (connector != null) {
            connector.disconnect();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (webSocketServer != null) {
            webSocketServer.stop();
        }
    }

    @Test
    @DisplayName("Full flow: WebSocket fixtures → Connector → Parser → Kafka → Consumer verifies ticks")
    void fullFlow_fixturesFlowThroughPipelineToKafka() throws Exception {
        // Start the pipeline: connector → Kafka producer
        kafkaProducer.start(btcUsdt);

        // Wait for messages to flow through the pipeline
        // WebSocket → Connector → Parser → Kafka Producer → Kafka Broker
        Thread.sleep(2000);

        // Read messages from the Kafka topic
        List<ConsumerRecord<String, NormalisedTick>> records = pollRecords(Duration.ofSeconds(10));

        // We sent 3 messages: 1 subscription ack + 2 bookTickers
        // Only 2 should reach Kafka (ack is filtered by the parser)
        assertFalse(records.isEmpty(), "Expected at least one record in Kafka topic");
        assertTrue(records.size() >= 2,
                "Expected 2 ticks in Kafka (sub ack filtered), got: " + records.size());

        // Verify first tick matches BOOK_TICKER_1
        NormalisedTick firstTick = records.get(0).value();
        assertEquals(ExchangeId.BINANCE, firstTick.getExchangeId());
        assertEquals("BTC", firstTick.getTradingPair().getBaseCurrency());
        assertEquals("USDT", firstTick.getTradingPair().getQuoteCurrency());
        assertEquals(0, new BigDecimal("67250.50000000").compareTo(firstTick.getBestBidPrice()));
        assertEquals(0, new BigDecimal("67251.30000000").compareTo(firstTick.getBestAskPrice()));
        assertEquals(0, new BigDecimal("1.23400000").compareTo(firstTick.getBestBidQuantity()));
        assertEquals(0, new BigDecimal("0.98700000").compareTo(firstTick.getBestAskQuantity()));

        // Verify Kafka message key is the canonical trading pair
        assertEquals("BTC-USDT", records.get(0).key());

        // Verify second tick matches BOOK_TICKER_2
        NormalisedTick secondTick = records.get(1).value();
        assertEquals(0, new BigDecimal("67255.10000000").compareTo(secondTick.getBestBidPrice()));
        assertEquals(0, new BigDecimal("67256.00000000").compareTo(secondTick.getBestAskPrice()));
        assertEquals(0, new BigDecimal("2.50000000").compareTo(secondTick.getBestBidQuantity()));
        assertEquals(0, new BigDecimal("1.10000000").compareTo(secondTick.getBestAskQuantity()));

        // Verify second tick also has BTC-USDT as key
        assertEquals("BTC-USDT", records.get(1).key());

        // Verify topic name
        assertEquals("raw-ticks-binance", records.get(0).topic());
    }

    @Test
    @DisplayName("Kafka message key is the canonical trading pair for partition affinity")
    void kafkaMessageKey_isCanonicalTradingPair() throws Exception {
        kafkaProducer.start(btcUsdt);
        Thread.sleep(2000);

        List<ConsumerRecord<String, NormalisedTick>> records = pollRecords(Duration.ofSeconds(10));
        assertFalse(records.isEmpty(), "Expected records in Kafka");

        // All records for the same trading pair should have the same key
        for (ConsumerRecord<String, NormalisedTick> record : records) {
            assertEquals("BTC-USDT", record.key(),
                    "All BTC-USDT ticks should use 'BTC-USDT' as message key");
        }
    }

    @Test
    @DisplayName("Timestamps are preserved through Kafka serialization/deserialization")
    void timestamps_arePreservedThroughKafka() throws Exception {
        kafkaProducer.start(btcUsdt);
        Thread.sleep(2000);

        List<ConsumerRecord<String, NormalisedTick>> records = pollRecords(Duration.ofSeconds(10));
        assertFalse(records.isEmpty(), "Expected records in Kafka");

        NormalisedTick tick = records.get(0).value();
        assertTrue(tick.getReceivedTimestamp() > 0,
                "T0 (receivedTimestamp) should survive serialization");
        assertTrue(tick.getProcessedTimestamp() > 0,
                "T1 (processedTimestamp) should survive serialization");
        assertTrue(tick.getProcessedTimestamp() > tick.getReceivedTimestamp(),
                "T1 should be after T0");
        assertTrue(tick.getExchangeTimestamp() != null,
                "exchangeTimestamp (Instant) should survive JSON serialization");
    }

    /**
     * Creates a Kafka consumer configured for the Testcontainers broker.
     *
     * <p><b>Key settings:</b></p>
     * <ul>
     *   <li>{@code auto.offset.reset=earliest} — read from the beginning of the topic,
     *       not just new messages. Without this, the consumer would miss messages produced
     *       before it subscribed.</li>
     *   <li>{@code group.id=test-consumer-group} — required by Kafka; each consumer must
     *       belong to a group.</li>
     *   <li>{@code JsonDeserializer} with trusted packages — Kafka's JSON deserializer
     *       refuses to deserialize classes from untrusted packages by default. We trust
     *       our domain packages.</li>
     * </ul>
     */
    private KafkaConsumer<String, NormalisedTick> createTestConsumer() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Use StringDeserializer for key, custom JsonDeserializer for value
        // The JsonDeserializer needs our ObjectMapper (with JavaTimeModule) to
        // handle Instant fields in NormalisedTick. Without JavaTimeModule, Jackson
        // cannot deserialize ISO-8601 timestamps back into java.time.Instant.
        StringDeserializer keyDeserializer = new StringDeserializer();

        JsonDeserializer<NormalisedTick> valueDeserializer = new JsonDeserializer<>(
                NormalisedTick.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");

        KafkaConsumer<String, NormalisedTick> testConsumer = new KafkaConsumer<>(
                consumerProps, keyDeserializer, valueDeserializer);
        testConsumer.subscribe(List.of(BinanceTickKafkaProducer.TOPIC));
        return testConsumer;
    }

    /**
     * Polls the Kafka consumer until at least one record arrives or timeout expires.
     *
     * <p>Multiple poll calls handle the case where the consumer needs time to join
     * the group and get partition assignments before records become available.</p>
     *
     * @param timeout maximum time to wait for records
     * @return all consumed records
     */
    private List<ConsumerRecord<String, NormalisedTick>> pollRecords(Duration timeout) {
        List<ConsumerRecord<String, NormalisedTick>> allRecords = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, NormalisedTick> batch = consumer.poll(Duration.ofMillis(500));
            batch.forEach(allRecords::add);
            if (!allRecords.isEmpty()) {
                // Give a short additional poll to catch any remaining messages
                ConsumerRecords<String, NormalisedTick> extra = consumer.poll(Duration.ofMillis(500));
                extra.forEach(allRecords::add);
                break;
            }
        }
        return allRecords;
    }
}
