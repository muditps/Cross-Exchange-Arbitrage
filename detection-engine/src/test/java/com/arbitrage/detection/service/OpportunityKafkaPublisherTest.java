package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionKafkaProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OpportunityKafkaPublisher}.
 *
 * <p>{@link KafkaTemplate} is mocked — these tests verify routing (topic, key, value)
 * without needing a Kafka broker. The integration test (Session 3.8) covers the live flow.
 */
@ExtendWith(MockitoExtension.class)
class OpportunityKafkaPublisherTest {

    @Mock
    private KafkaTemplate<String, ArbitrageOpportunity> kafkaTemplate;

    private OpportunityKafkaPublisher publisher;

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");

    @BeforeEach
    void setUp() {
        publisher = new OpportunityKafkaPublisher(kafkaTemplate);
        // lenient: the failure test overrides this with a failed future
        lenient().when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    @DisplayName("publish sends to the arbitrage-opportunities topic")
    void publish_sendsToCorrectTopic() {
        ArbitrageOpportunity opp = opportunity(BTC_USDT, OpportunityStatus.DETECTED);

        publisher.publish(opp, null);

        ArgumentCaptor<ProducerRecord> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertThat(recordCaptor.getValue().topic())
                .isEqualTo(DetectionKafkaProducerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES);
        assertThat(recordCaptor.getValue().value()).isEqualTo(opp);
    }

    @Test
    @DisplayName("partition key is the canonical trading pair symbol")
    void publish_usesCanonicalPairSymbolAsKey() {
        ArbitrageOpportunity opp = opportunity(BTC_USDT, OpportunityStatus.DETECTED);

        publisher.publish(opp, null);

        ArgumentCaptor<ProducerRecord> keyCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(keyCaptor.capture());
        assertThat(keyCaptor.getValue().key()).isEqualTo("BTC-USDT");
    }

    @Test
    @DisplayName("publish does not throw when Kafka send fails — errors are logged only")
    void publish_kafkaFailure_doesNotThrow() {
        ArbitrageOpportunity opp = opportunity(BTC_USDT, OpportunityStatus.DETECTED);
        CompletableFuture<SendResult<String, ArbitrageOpportunity>> failed =
                CompletableFuture.failedFuture(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        // Should not throw — errors are handled in the whenComplete callback
        publisher.publish(opp, null);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private ArbitrageOpportunity opportunity(TradingPair pair, OpportunityStatus status) {
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(pair)
                .sellExchange(ExchangeId.BINANCE)
                .buyExchange(ExchangeId.KUCOIN)
                .sellPrice(new BigDecimal("65200.00"))
                .buyPrice(new BigDecimal("65000.00"))
                .sellQuantity(new BigDecimal("1.00000000"))
                .buyQuantity(new BigDecimal("1.00000000"))
                .sellFeeRate(ExchangeId.BINANCE.getDefaultTakerFeeRate())
                .buyFeeRate(ExchangeId.KUCOIN.getDefaultTakerFeeRate())
                .grossSpread(new BigDecimal("200.00"))
                .netSpread(new BigDecimal("69.80"))
                .grossSpreadBps(new BigDecimal("30.76923077"))
                .netSpreadBps(new BigDecimal("10.73846154"))
                .arbitrageableQuantity(new BigDecimal("1.00000000"))
                .theoreticalProfit(new BigDecimal("69.80"))
                .status(status)
                .detectionTimestamp(Instant.now())
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(System.nanoTime())
                .closedNanoTime(0L)
                .peakNetSpread(new BigDecimal("69.80"))
                .averageNetSpread(new BigDecimal("69.80"))
                .totalDurationMs(0L)
                .updateCount(1L)
                .build();
    }
}
