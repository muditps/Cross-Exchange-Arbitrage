package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.arbitrage.normalisation.transformer.TickTransformerTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BybitTickTransformer}.
 *
 * <p>Verifies the transformer's core contract — same as Binance, with the
 * additional note that Bybit provides a real server timestamp ({@code ts}) so
 * {@code exchangeTimestamp} is meaningful (not {@code Instant.now()}).
 */
class BybitTickTransformerTest {

    private BybitTickTransformer transformer;
    private static final long START_NANOS = 999_000_000L;

    @BeforeEach
    void setUp() {
        transformer = new BybitTickTransformer();
    }

    @Test
    @DisplayName("supports() returns BYBIT")
    void supports_returnsBybit() {
        assertThat(transformer.supports()).isEqualTo(ExchangeId.BYBIT);
    }

    @Nested
    @DisplayName("Happy Path")
    class HappyPath {

        @Test
        @DisplayName("Returns non-empty Optional for a valid Bybit tick")
        void transform_validTick_returnsNonEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS)).isPresent();
        }

        @Test
        @DisplayName("exchangeId is preserved as BYBIT")
        void transform_preservesExchangeId() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS).orElseThrow();
            assertThat(result.getExchangeId()).isEqualTo(ExchangeId.BYBIT);
        }

        @Test
        @DisplayName("bestBidPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesBidPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS).orElseThrow();
            assertThat(result.getBestBidPrice()).isEqualByComparingTo(BTC_BID);
            assertThat(result.getBestBidPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("bestAskPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesAskPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS).orElseThrow();
            assertThat(result.getBestAskPrice()).isEqualByComparingTo(BTC_ASK);
            assertThat(result.getBestAskPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("exchangeTimestamp is preserved — Bybit provides real server timestamp")
        void transform_preservesExchangeTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS).orElseThrow();
            assertThat(result.getExchangeTimestamp()).isEqualTo(FIXED_EXCHANGE_TIMESTAMP);
        }

        @Test
        @DisplayName("receivedTimestamp (T0) is preserved — staleness anchor must not change")
        void transform_preservesReceivedTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS).orElseThrow();
            assertThat(result.getReceivedTimestamp()).isEqualTo(FIXED_RECEIVED_NANOS);
        }

        @Test
        @DisplayName("processedTimestamp (T4) is updated to a value after the inbound processedTimestamp")
        void transform_updatesProcessedTimestamp() {
            NormalisedTick inbound = buildBtcTick(ExchangeId.BYBIT);
            NormalisedTick result = transformer.transform(inbound, START_NANOS).orElseThrow();
            assertThat(result.getProcessedTimestamp()).isGreaterThan(inbound.getProcessedTimestamp());
        }
    }

    @Nested
    @DisplayName("Wrong Exchange Guard")
    class WrongExchangeTests {

        @Test
        @DisplayName("Returns empty for BINANCE tick")
        void transform_binanceTick_returnsEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Returns empty for KUCOIN tick")
        void transform_kucoinTick_returnsEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Invalid Input Guard")
    class InvalidInputTests {

        @Test
        @DisplayName("Returns empty for null tick")
        void transform_nullTick_returnsEmpty() {
            assertThat(transformer.transform(null, START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Returns empty when bestBidQuantity is null")
        void transform_nullBidQuantity_returnsEmpty() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BYBIT)
                    .tradingPair(BTC_USDT)
                    .bestBidPrice(BTC_BID)
                    .bestAskPrice(BTC_ASK)
                    .bestBidQuantity(null)
                    .bestAskQuantity(BTC_ASK_QTY)
                    .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                    .receivedTimestamp(FIXED_RECEIVED_NANOS)
                    .processedTimestamp(FIXED_PROCESSED_NANOS)
                    .build();
            assertThat(transformer.transform(tick, START_NANOS)).isEmpty();
        }
    }
}
