package com.arbitrage.connector.binance;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BinanceTickKafkaProducer}.
 *
 * <p>Uses Mockito to mock both {@link BinanceConnector} and {@link KafkaTemplate}.
 * The connector emits ticks via a Reactor Sink (giving us control over when ticks
 * appear), and the Kafka template captures sent messages for assertion.</p>
 *
 * <p>These are pure unit tests — no Kafka broker. The Testcontainers integration
 * test with a real Kafka broker is created in Session 1.7.</p>
 */
@ExtendWith(MockitoExtension.class)
class BinanceTickKafkaProducerTest {

    @Mock
    private BinanceConnector mockConnector;

    @Mock
    private KafkaTemplate<String, NormalisedTick> mockKafkaTemplate;

    private TradingPair btcUsdt;
    private Sinks.Many<NormalisedTick> tickSink;
    private BinanceTickKafkaProducer producer;

    @BeforeEach
    void setUp() {
        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();

        // Create a controllable tick source — we emit ticks manually via the sink
        tickSink = Sinks.many().multicast().onBackpressureBuffer();
    }

    /**
     * Configures the mock connector to return our controllable tick sink.
     * Called only by tests that actually start the producer (avoids UnnecessaryStubbingException).
     */
    private void stubConnector() {
        when(mockConnector.connect(any(TradingPair.class))).thenReturn(tickSink.asFlux());
    }

    /**
     * Creates a realistic NormalisedTick for testing.
     */
    private NormalisedTick createTestTick(TradingPair pair) {
        return NormalisedTick.builder()
                .exchangeId(ExchangeId.BINANCE)
                .tradingPair(pair)
                .bestBidPrice(new BigDecimal("67250.50000000"))
                .bestAskPrice(new BigDecimal("67251.30000000"))
                .bestBidQuantity(new BigDecimal("1.23400000"))
                .bestAskQuantity(new BigDecimal("0.98700000"))
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }

    /**
     * Creates a mock successful SendResult for KafkaTemplate.
     */
    private CompletableFuture<SendResult<String, NormalisedTick>> successFuture() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(BinanceTickKafkaProducer.TOPIC, 0),
                0L, 0, System.currentTimeMillis(), 0, 0);
        SendResult<String, NormalisedTick> result = new SendResult<>(
                new ProducerRecord<>(BinanceTickKafkaProducer.TOPIC, "BTC-USDT", null),
                metadata);
        return CompletableFuture.completedFuture(result);
    }

    /**
     * Creates a mock failed SendResult for KafkaTemplate.
     */
    private CompletableFuture<SendResult<String, NormalisedTick>> failureFuture() {
        CompletableFuture<SendResult<String, NormalisedTick>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka broker unavailable"));
        return future;
    }

    @Nested
    @DisplayName("Lifecycle Management")
    class LifecycleTests {

        @Test
        @DisplayName("start() subscribes to connector and sets isRunning to true")
        void start_subscribesAndSetsRunning() {
            stubConnector();
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);

            producer.start(btcUsdt);

            assertTrue(producer.isRunning(), "Producer should be running after start");
            verify(mockConnector).connect(btcUsdt);
        }

        @Test
        @DisplayName("stop() disposes subscription and sets isRunning to false")
        void stop_disposesSubscription() {
            stubConnector();
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);

            producer.stop();

            assertFalse(producer.isRunning(), "Producer should not be running after stop");
        }

        @Test
        @DisplayName("start() is idempotent — second call while running is ignored")
        void start_isIdempotent() {
            stubConnector();
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);
            producer.start(btcUsdt);

            assertTrue(producer.isRunning());
            // connect() should only be called once
            verify(mockConnector).connect(btcUsdt);
        }

        @Test
        @DisplayName("stop() is idempotent — calling when stopped has no effect")
        void stop_isIdempotent() {
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);

            producer.stop();
            producer.stop();

            assertFalse(producer.isRunning());
        }

        @Test
        @DisplayName("Disabled producer does not connect or subscribe")
        void disabled_doesNotConnect() {
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, false);

            producer.start(btcUsdt);

            assertFalse(producer.isRunning());
            verify(mockConnector, never()).connect(any());
        }

        @Test
        @DisplayName("isRunning is false before start")
        void isRunning_falseBeforeStart() {
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);

            assertFalse(producer.isRunning());
        }
    }

    @Nested
    @DisplayName("Kafka Publishing")
    class PublishingTests {

        @Test
        @DisplayName("Tick is published to correct topic with trading pair as key")
        void tick_publishedWithCorrectTopicAndKey() {
            stubConnector();
            when(mockKafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture());

            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);

            NormalisedTick tick = createTestTick(btcUsdt);
            tickSink.tryEmitNext(tick);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<ProducerRecord<String, NormalisedTick>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(mockKafkaTemplate, timeout(1000)).send(recordCaptor.capture());

            ProducerRecord<String, NormalisedTick> record = recordCaptor.getValue();
            assertEquals(BinanceTickKafkaProducer.TOPIC, record.topic());
            assertEquals("BTC-USDT", record.key());
            assertEquals(ExchangeId.BINANCE, record.value().getExchangeId());
            assertEquals(0, new BigDecimal("67250.50000000").compareTo(record.value().getBestBidPrice()));
        }

        @Test
        @DisplayName("Multiple ticks are all published")
        void multipleTicks_allPublished() {
            stubConnector();
            when(mockKafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture());

            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);

            tickSink.tryEmitNext(createTestTick(btcUsdt));
            tickSink.tryEmitNext(createTestTick(btcUsdt));
            tickSink.tryEmitNext(createTestTick(btcUsdt));

            verify(mockKafkaTemplate, timeout(1000).times(3)).send(any(ProducerRecord.class));
        }

        @Test
        @DisplayName("Different trading pairs use different Kafka keys")
        void differentPairs_differentKeys() {
            stubConnector();
            when(mockKafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture());

            TradingPair ethUsdt = TradingPair.builder()
                    .baseCurrency("ETH")
                    .quoteCurrency("USDT")
                    .build();

            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);

            // Emit a tick with ETH-USDT pair (the connector could serve multiple pairs)
            NormalisedTick ethTick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(ethUsdt)
                    .bestBidPrice(new BigDecimal("3456.78"))
                    .bestAskPrice(new BigDecimal("3456.99"))
                    .bestBidQuantity(new BigDecimal("15.678"))
                    .bestAskQuantity(new BigDecimal("8.456"))
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            tickSink.tryEmitNext(ethTick);

            @SuppressWarnings("unchecked")
            ArgumentCaptor<ProducerRecord<String, NormalisedTick>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(mockKafkaTemplate, timeout(1000)).send(recordCaptor.capture());
            assertEquals("ETH-USDT", recordCaptor.getValue().key());
        }

        @Test
        @DisplayName("Topic name is 'raw-ticks-binance'")
        void topicName_isCorrect() {
            assertEquals("raw-ticks-binance", BinanceTickKafkaProducer.TOPIC);
        }
    }

    @Nested
    @DisplayName("Error Resilience")
    class ErrorResilienceTests {

        @Test
        @DisplayName("Kafka send failure does not stop the subscription")
        void sendFailure_doesNotStopSubscription() {
            stubConnector();
            // First send fails, second succeeds
            when(mockKafkaTemplate.send(any(ProducerRecord.class)))
                    .thenReturn(failureFuture())
                    .thenReturn(successFuture());

            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);

            tickSink.tryEmitNext(createTestTick(btcUsdt));
            tickSink.tryEmitNext(createTestTick(btcUsdt));

            // Both sends should be attempted — failure of first doesn't cancel subscription
            verify(mockKafkaTemplate, timeout(1000).times(2)).send(any(ProducerRecord.class));

            assertTrue(producer.isRunning(), "Producer should still be running after send failure");
        }

        @Test
        @DisplayName("No ticks published after stop")
        void afterStop_noTicksPublished() {
            stubConnector();
            producer = new BinanceTickKafkaProducer(mockConnector, mockKafkaTemplate, true);
            producer.start(btcUsdt);
            producer.stop();

            tickSink.tryEmitNext(createTestTick(btcUsdt));

            // Small delay to ensure any async processing would have happened
            verify(mockKafkaTemplate, timeout(200).times(0)).send(any(ProducerRecord.class));
        }
    }
}
