package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.normalisation.config.NormalisationKafkaProducerConfig;
import com.arbitrage.normalisation.transformer.TickTransformerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NormalisationService}.
 *
 * <p>All dependencies are mocked — this is a pure unit test of the service's
 * orchestration logic: routing, acknowledgment timing, metrics calls, and error handling.
 * No Kafka broker, no Spring context required.
 *
 * <p><b>Key correctness invariants tested:</b>
 * <ul>
 *   <li>Acknowledgment is NEVER called before produce succeeds (at-least-once guarantee)</li>
 *   <li>Dropped ticks are acknowledged immediately (bad data must not block the pipeline)</li>
 *   <li>Publish failure means no acknowledgment (broker error triggers redelivery)</li>
 *   <li>T0 (receivedTimestamp) is never modified by the service</li>
 *   <li>Kafka message key is always the canonical symbol (pair routing requirement)</li>
 *   <li>Metrics are recorded only for the outcomes they should measure</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NormalisationService")
class NormalisationServiceTest {

    @Mock
    private TickTransformerFactory transformerFactory;

    @Mock
    private NormalisationMetrics metrics;

    @Mock
    private FeedHealthMonitor feedHealthMonitor;

    @Mock
    private ClockSkewMonitor clockSkewMonitor;

    @Mock
    private KafkaTemplate<String, NormalisedTick> kafkaTemplate;

    @Mock
    private Acknowledgment acknowledgment;

    private NormalisationService service;

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");
    private static final long RECEIVED_NANOS = 1_000_000_000L;
    private static final long PROCESSED_NANOS = 1_000_050_000L; // T4-T3 = 50µs

    @BeforeEach
    void setUp() {
        // Constructor injection — bypasses Spring, tests just the orchestration logic
        service = new NormalisationService(transformerFactory, metrics, feedHealthMonitor, clockSkewMonitor, kafkaTemplate);
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private NormalisedTick buildInboundTick(ExchangeId exchangeId) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(BTC_USDT)
                .bestBidPrice(new BigDecimal("67250.50000000"))
                .bestAskPrice(new BigDecimal("67251.30000000"))
                .bestBidQuantity(new BigDecimal("1.23400000"))
                .bestAskQuantity(new BigDecimal("0.98700000"))
                .exchangeTimestamp(Instant.ofEpochMilli(1673853746003L))
                .receivedTimestamp(RECEIVED_NANOS)
                .processedTimestamp(0L)
                .build();
    }

    private NormalisedTick buildTransformedTick(ExchangeId exchangeId) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(BTC_USDT)
                .bestBidPrice(new BigDecimal("67250.50000000"))
                .bestAskPrice(new BigDecimal("67251.30000000"))
                .bestBidQuantity(new BigDecimal("1.23400000"))
                .bestAskQuantity(new BigDecimal("0.98700000"))
                .exchangeTimestamp(Instant.ofEpochMilli(1673853746003L))
                .receivedTimestamp(RECEIVED_NANOS) // T0 preserved
                .processedTimestamp(PROCESSED_NANOS) // T4 stamped by transformer
                .build();
    }

    /**
     * Creates a successful CompletableFuture that mimics a Kafka produce success.
     * Uses a real RecordMetadata so assertions on partition/offset can work.
     */
    private CompletableFuture<SendResult<String, NormalisedTick>> successFuture(NormalisedTick tick) {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS, 0),
                0L, 0, 0L, 0, 0);
        SendResult<String, NormalisedTick> sendResult = new SendResult<>(null, metadata);
        return CompletableFuture.completedFuture(sendResult);
    }

    /**
     * Creates a failed CompletableFuture that mimics a Kafka broker error.
     */
    private CompletableFuture<SendResult<String, NormalisedTick>> failedFuture() {
        CompletableFuture<SendResult<String, NormalisedTick>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Simulated broker error"));
        return future;
    }

    // ========================================================================
    // Happy path
    // ========================================================================

    @Nested
    @DisplayName("happy path — valid tick, transformer succeeds, publish succeeds")
    class HappyPath {

        @Test
        @DisplayName("calls transformer with the inbound tick")
        void callsTransformerWithInboundTick() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(transformerFactory).transform(eq(inbound), anyLong());
        }

        @Test
        @DisplayName("publishes to the correct topic")
        void publishesToNormalisedTicksTopic() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(kafkaTemplate).send(argThat((ProducerRecord<String, NormalisedTick> r) ->
                    NormalisationKafkaProducerConfig.TOPIC_NORMALISED_TICKS.equals(r.topic())));
        }

        @Test
        @DisplayName("uses canonical symbol as Kafka message key")
        void usesCanonicalSymbolAsMessageKey() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(kafkaTemplate).send(argThat((ProducerRecord<String, NormalisedTick> r) ->
                    "BINANCE:BTC-USDT".equals(r.key())));
        }

        @Test
        @DisplayName("publishes the transformed tick (not the inbound tick)")
        void publishesTransformedTickNotInboundTick() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            // Capture via ProducerRecord — value is the published tick
            ArgumentCaptor<ProducerRecord> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate).send(recordCaptor.capture());
            NormalisedTick published = (NormalisedTick) recordCaptor.getValue().value();
            assertThat(published.getProcessedTimestamp())
                    .isEqualTo(PROCESSED_NANOS); // T4 from transformer, not 0L from inbound
        }

        @Test
        @DisplayName("acknowledges offset only after successful publish (T5)")
        void acknowledgesOffsetOnlyAfterSuccessfulPublish() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BYBIT);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BYBIT);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("records normalised tick metric on successful publish")
        void recordsNormalisedTickMetricOnSuccess() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.KUCOIN);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.KUCOIN);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(metrics).recordNormalisedTick(ExchangeId.KUCOIN);
        }

        @Test
        @DisplayName("records processing duration metric (T4-T3)")
        void recordsProcessingDurationMetric() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            // Duration = T4 - T3 = PROCESSED_NANOS - normalisationStartNanos
            // We can't assert the exact value (T3 is System.nanoTime()),
            // but we can verify the method was called with the correct exchange
            verify(metrics).recordProcessingDuration(eq(ExchangeId.BINANCE), anyLong());
        }

        @Test
        @DisplayName("notifies ClockSkewMonitor with exchange timestamp after successful transformation")
        void notifiesClockSkewMonitorOnSuccessfulTransformation() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(clockSkewMonitor).recordTick(ExchangeId.BINANCE, transformed.getExchangeTimestamp());
        }

        @Test
        @DisplayName("notifies FeedHealthMonitor of tick arrival after successful transformation")
        void notifiesFeedHealthMonitorOnSuccessfulTransformation() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            verify(feedHealthMonitor).recordTickReceived(ExchangeId.BINANCE);
        }

        @Test
        @DisplayName("preserves T0 (receivedTimestamp) in the published tick")
        void preservesReceivedTimestampInPublishedTick() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(successFuture(transformed));

            service.onRawTick(inbound, acknowledgment);

            ArgumentCaptor<ProducerRecord> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate).send(recordCaptor.capture());
            NormalisedTick published = (NormalisedTick) recordCaptor.getValue().value();
            assertThat(published.getReceivedTimestamp())
                    .isEqualTo(RECEIVED_NANOS); // T0 never modified by the service
        }
    }

    // ========================================================================
    // Null tick (deserialization failure)
    // ========================================================================

    @Nested
    @DisplayName("null tick — deserialization failure")
    class NullTickHandling {

        @Test
        @DisplayName("acknowledges null tick without publishing")
        void acknowledgesNullTickWithoutPublishing() {
            service.onRawTick((NormalisedTick) null, acknowledgment);

            verify(acknowledgment, times(1)).acknowledge();
            verifyNoInteractions(kafkaTemplate);
        }

        @Test
        @DisplayName("records dropped metric with null_tick reason")
        void recordsDroppedMetricWithNullTickReason() {
            service.onRawTick((NormalisedTick) null, acknowledgment);

            verify(metrics).recordDroppedTick(null, NormalisationMetrics.DROP_REASON_NULL_TICK);
        }

        @Test
        @DisplayName("does not call transformer for null tick")
        void doesNotCallTransformerForNullTick() {
            service.onRawTick((NormalisedTick) null, acknowledgment);

            verifyNoInteractions(transformerFactory);
        }

        @Test
        @DisplayName("does not notify FeedHealthMonitor for null tick")
        void doesNotNotifyFeedHealthMonitorForNullTick() {
            service.onRawTick((NormalisedTick) null, acknowledgment);

            verifyNoInteractions(feedHealthMonitor);
        }
    }

    // ========================================================================
    // Transformer returns empty — invalid fields
    // ========================================================================

    @Nested
    @DisplayName("transformer returns empty — invalid fields")
    class InvalidFieldsHandling {

        @Test
        @DisplayName("acknowledges tick without publishing when transformer returns empty")
        void acknowledgesTickWithoutPublishingWhenTransformerReturnsEmpty() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            when(transformerFactory.supports(ExchangeId.BINANCE)).thenReturn(true);
            when(transformerFactory.transform(eq(inbound), anyLong())).thenReturn(Optional.empty());

            service.onRawTick(inbound, acknowledgment);

            verify(acknowledgment, times(1)).acknowledge();
            verifyNoInteractions(kafkaTemplate);
        }

        @Test
        @DisplayName("records dropped metric with invalid_fields reason when transformer is registered")
        void recordsDroppedMetricWithInvalidFieldsReasonWhenTransformerRegistered() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BYBIT);
            when(transformerFactory.supports(ExchangeId.BYBIT)).thenReturn(true);
            when(transformerFactory.transform(eq(inbound), anyLong())).thenReturn(Optional.empty());

            service.onRawTick(inbound, acknowledgment);

            verify(metrics).recordDroppedTick(ExchangeId.BYBIT, NormalisationMetrics.DROP_REASON_INVALID_FIELDS);
        }

        @Test
        @DisplayName("records dropped metric with no_transformer reason when transformer is not registered")
        void recordsDroppedMetricWithNoTransformerReasonWhenNotRegistered() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.KUCOIN);
            when(transformerFactory.supports(ExchangeId.KUCOIN)).thenReturn(false);
            when(transformerFactory.transform(eq(inbound), anyLong())).thenReturn(Optional.empty());

            service.onRawTick(inbound, acknowledgment);

            verify(metrics).recordDroppedTick(ExchangeId.KUCOIN, NormalisationMetrics.DROP_REASON_NO_TRANSFORMER);
        }

        @Test
        @DisplayName("does not record normalised tick metric when tick is dropped")
        void doesNotRecordNormalisedMetricWhenDropped() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            when(transformerFactory.supports(ExchangeId.BINANCE)).thenReturn(true);
            when(transformerFactory.transform(eq(inbound), anyLong())).thenReturn(Optional.empty());

            service.onRawTick(inbound, acknowledgment);

            verify(metrics, never()).recordNormalisedTick(any());
        }

        @Test
        @DisplayName("does not notify FeedHealthMonitor when transformer returns empty")
        void doesNotNotifyFeedHealthMonitorWhenTransformerReturnsEmpty() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BYBIT);
            when(transformerFactory.supports(ExchangeId.BYBIT)).thenReturn(true);
            when(transformerFactory.transform(eq(inbound), anyLong())).thenReturn(Optional.empty());

            service.onRawTick(inbound, acknowledgment);

            verifyNoInteractions(feedHealthMonitor);
        }
    }

    // ========================================================================
    // Kafka publish failure
    // ========================================================================

    @Nested
    @DisplayName("Kafka publish failure — at-least-once guarantee")
    class PublishFailureHandling {

        @Test
        @DisplayName("does NOT acknowledge when publish fails — Kafka will redeliver")
        void doesNotAcknowledgeWhenPublishFails() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BINANCE);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BINANCE);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture());

            service.onRawTick(inbound, acknowledgment);

            verifyNoInteractions(acknowledgment);
        }

        @Test
        @DisplayName("does NOT record normalised tick metric when publish fails")
        void doesNotRecordNormalisedMetricWhenPublishFails() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.BYBIT);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.BYBIT);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture());

            service.onRawTick(inbound, acknowledgment);

            verify(metrics, never()).recordNormalisedTick(any());
        }

        @Test
        @DisplayName("still records processing duration metric even when publish fails")
        void recordsProcessingDurationEvenWhenPublishFails() {
            NormalisedTick inbound = buildInboundTick(ExchangeId.KUCOIN);
            NormalisedTick transformed = buildTransformedTick(ExchangeId.KUCOIN);
            when(transformerFactory.transform(eq(inbound), anyLong()))
                    .thenReturn(Optional.of(transformed));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture());

            service.onRawTick(inbound, acknowledgment);

            // Processing duration is recorded before the async publish,
            // so it is always captured regardless of publish outcome
            verify(metrics).recordProcessingDuration(eq(ExchangeId.KUCOIN), anyLong());
        }
    }
}
