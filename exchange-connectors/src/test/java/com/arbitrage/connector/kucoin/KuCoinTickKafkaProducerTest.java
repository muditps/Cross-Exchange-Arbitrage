package com.arbitrage.connector.kucoin;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KuCoinTickKafkaProducer}.
 *
 * <p>Tests producer start/stop lifecycle, Kafka send behaviour, idempotency,
 * and disabled-state handling. The KuCoin connector is exercised via
 * {@link KuCoinConnector#processMessage} to inject test ticks.</p>
 */
@SuppressWarnings("unchecked")
class KuCoinTickKafkaProducerTest {

    private static final String VALID_TICKER =
            "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\",\"data\":{"
                    + "\"bestBid\":\"21109.50000000\",\"bestBidSize\":\"0.50000000\","
                    + "\"bestAsk\":\"21109.60000000\",\"bestAskSize\":\"0.30000000\","
                    + "\"time\":1673853746003}}";

    private KuCoinConnector connector;
    private KafkaTemplate<String, NormalisedTick> kafkaTemplate;
    private KuCoinTickKafkaProducer producer;
    private TradingPair btcUsdt;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        com.arbitrage.connector.ExchangeConnectorProperties.ExchangeProperties config =
                new com.arbitrage.connector.ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint("https://api.kucoin.com/api/v1/bullet-public");
        config.setTakerFeeRate(new BigDecimal("0.0010"));
        config.setInitialReconnectDelay(Duration.ofSeconds(1));
        config.setMaxReconnectDelay(Duration.ofSeconds(30));
        config.setMaxReconnectAttempts(10);
        config.setStalenessThreshold(Duration.ofMillis(500));
        config.setEnabled(true);

        WebSocketClient mockWsClient = mock(WebSocketClient.class);
        when(mockWsClient.execute(any(URI.class), any())).thenReturn(Mono.never());

        KuCoinConnectionDetails details = new KuCoinConnectionDetails(
                "test-token", "wss://ws-api.kucoin.com/endpoint", 18000L);
        KuCoinTokenService mockTokenService = new KuCoinTokenService(WebClient.create(), "http://localhost/unused") {
            @Override
            public synchronized Mono<KuCoinConnectionDetails> getConnectionDetails() {
                return Mono.just(details);
            }
        };

        ObjectMapper objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        KuCoinMessageParser messageParser = new KuCoinMessageParser(objectMapper, metrics);
        connector = new KuCoinConnector(config, mockTokenService, messageParser, mockWsClient, metrics);

        kafkaTemplate = mock(KafkaTemplate.class);
        producer = new KuCoinTickKafkaProducer(connector, kafkaTemplate, true);

        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    // ============================================================
    // Topic Configuration
    // ============================================================

    @Nested
    @DisplayName("Topic Configuration")
    class TopicConfigTests {

        @Test
        @DisplayName("Kafka topic is raw-ticks-kucoin")
        void topic_isRawTicksKucoin() {
            assertEquals("raw-ticks-kucoin", KuCoinTickKafkaProducer.TOPIC);
        }
    }

    // ============================================================
    // Start / Stop Lifecycle
    // ============================================================

    @Nested
    @DisplayName("Start / Stop Lifecycle")
    class LifecycleTests {

        @Test
        @DisplayName("Producer is not running before start")
        void producer_isNotRunningBeforeStart() {
            assertFalse(producer.isRunning());
        }

        @Test
        @DisplayName("Producer is running after start")
        void producer_isRunningAfterStart() {
            producer.start(btcUsdt);
            assertTrue(producer.isRunning());
        }

        @Test
        @DisplayName("Producer is not running after stop")
        void producer_isNotRunningAfterStop() {
            producer.start(btcUsdt);
            producer.stop();
            assertFalse(producer.isRunning());
        }

        @Test
        @DisplayName("start() is idempotent — calling twice has no effect")
        void start_isIdempotent() {
            producer.start(btcUsdt);
            producer.start(btcUsdt);
            assertTrue(producer.isRunning());
        }

        @Test
        @DisplayName("stop() is idempotent — calling twice does not throw")
        void stop_isIdempotent() {
            producer.start(btcUsdt);
            producer.stop();
            producer.stop();
            assertFalse(producer.isRunning());
        }
    }

    // ============================================================
    // Kafka Publishing
    // ============================================================

    @Nested
    @DisplayName("Kafka Publishing")
    class PublishingTests {

        @BeforeEach
        void setUpSuccessfulSend() {
            ProducerRecord<String, NormalisedTick> record =
                    new ProducerRecord<>(KuCoinTickKafkaProducer.TOPIC, "BTC-USDT", null);
            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition(KuCoinTickKafkaProducer.TOPIC, 0),
                    0, 0, 0, 0, 0);
            SendResult<String, NormalisedTick> sendResult = new SendResult<>(record, metadata);
            CompletableFuture<SendResult<String, NormalisedTick>> future =
                    CompletableFuture.completedFuture(sendResult);

            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);
        }

        @Test
        @DisplayName("Tick from connector is sent to Kafka with canonical symbol as key")
        void tick_isSentToKafkaWithCorrectKey() {
            producer.start(btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            ArgumentCaptor<ProducerRecord<String, NormalisedTick>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate, times(1)).send(recordCaptor.capture());
            assertEquals(KuCoinTickKafkaProducer.TOPIC, recordCaptor.getValue().topic());
            assertEquals("BTC-USDT", recordCaptor.getValue().key());
        }

        @Test
        @DisplayName("Multiple ticks result in multiple Kafka sends")
        void multipleTicks_resultInMultipleSends() {
            producer.start(btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            verify(kafkaTemplate, times(2)).send(any(ProducerRecord.class));
        }

        @Test
        @DisplayName("Stop cancels subscription — no more sends after stop")
        void stopCancelsSubscription_noMoreSendsAfterStop() {
            producer.start(btcUsdt);
            producer.stop();
            connector.processMessage(VALID_TICKER, btcUsdt);

            verifyNoInteractions(kafkaTemplate);
        }
    }

    // ============================================================
    // Disabled Producer Tests
    // ============================================================

    @Nested
    @DisplayName("Disabled Producer")
    class DisabledProducerTests {

        @Test
        @DisplayName("Disabled producer does not start")
        void disabledProducer_doesNotStart() {
            KuCoinTickKafkaProducer disabledProducer =
                    new KuCoinTickKafkaProducer(connector, kafkaTemplate, false);

            disabledProducer.start(btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            verifyNoInteractions(kafkaTemplate);
        }

        @Test
        @DisplayName("Disabled producer isRunning() returns false")
        void disabledProducer_isNotRunning() {
            KuCoinTickKafkaProducer disabledProducer =
                    new KuCoinTickKafkaProducer(connector, kafkaTemplate, false);

            disabledProducer.start(btcUsdt);

            assertFalse(disabledProducer.isRunning());
        }
    }
}
