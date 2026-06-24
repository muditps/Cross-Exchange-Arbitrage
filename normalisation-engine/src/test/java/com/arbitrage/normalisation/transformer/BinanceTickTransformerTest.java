package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static com.arbitrage.normalisation.transformer.TickTransformerTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BinanceTickTransformer}.
 *
 * <p>Verifies the transformer's core contract:
 * <ul>
 *   <li>Price and quantity fields pass through with no precision loss</li>
 *   <li>receivedTimestamp (T0) is preserved unchanged</li>
 *   <li>processedTimestamp (T4) is updated to a later nanoTime</li>
 *   <li>Wrong exchange or null/invalid input returns Optional.empty()</li>
 * </ul>
 */
class BinanceTickTransformerTest {

    private BinanceTickTransformer transformer;
    private static final long START_NANOS = 999_000_000L;

    @BeforeEach
    void setUp() {
        transformer = new BinanceTickTransformer();
    }

    @Test
    @DisplayName("supports() returns BINANCE")
    void supports_returnsBinance() {
        assertThat(transformer.supports()).isEqualTo(ExchangeId.BINANCE);
    }

    @Nested
    @DisplayName("Happy Path")
    class HappyPath {

        @Test
        @DisplayName("Returns non-empty Optional for a valid Binance tick")
        void transform_validTick_returnsNonEmpty() {
            NormalisedTick inbound = buildBtcTick(ExchangeId.BINANCE);
            Optional<NormalisedTick> result = transformer.transform(inbound, START_NANOS);
            assertThat(result).isPresent();
        }

        @Test
        @DisplayName("exchangeId is preserved as BINANCE")
        void transform_preservesExchangeId() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getExchangeId()).isEqualTo(ExchangeId.BINANCE);
        }

        @Test
        @DisplayName("tradingPair is preserved")
        void transform_preservesTradingPair() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getTradingPair()).isEqualTo(BTC_USDT);
        }

        @Test
        @DisplayName("bestBidPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesBidPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestBidPrice()).isEqualByComparingTo(BTC_BID);
            assertThat(result.getBestBidPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("bestAskPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesAskPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestAskPrice()).isEqualByComparingTo(BTC_ASK);
            assertThat(result.getBestAskPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("bestBidQuantity passes through unchanged")
        void transform_preservesBidQuantity() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestBidQuantity()).isEqualByComparingTo(BTC_BID_QTY);
        }

        @Test
        @DisplayName("bestAskQuantity passes through unchanged")
        void transform_preservesAskQuantity() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestAskQuantity()).isEqualByComparingTo(BTC_ASK_QTY);
        }

        @Test
        @DisplayName("exchangeTimestamp is preserved unchanged")
        void transform_preservesExchangeTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getExchangeTimestamp()).isEqualTo(FIXED_EXCHANGE_TIMESTAMP);
        }

        @Test
        @DisplayName("receivedTimestamp (T0) is preserved — staleness anchor must not change")
        void transform_preservesReceivedTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getReceivedTimestamp()).isEqualTo(FIXED_RECEIVED_NANOS);
        }

        @Test
        @DisplayName("processedTimestamp (T4) is updated to a value after the inbound processedTimestamp")
        void transform_updatesProcessedTimestamp() {
            NormalisedTick inbound = buildBtcTick(ExchangeId.BINANCE);
            NormalisedTick result = transformer.transform(inbound, START_NANOS).orElseThrow();
            assertThat(result.getProcessedTimestamp()).isGreaterThan(inbound.getProcessedTimestamp());
        }
    }

    @Nested
    @DisplayName("Precision: Micro-Price Pass-Through")
    class PrecisionTests {

        @Test
        @DisplayName("SHIB micro-price bid 0.00002834 passes through without precision loss")
        void transform_shibBidPrice_exactPrecision() {
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestBidPrice()).isEqualByComparingTo(SHIB_BID);
            assertThat(result.getBestBidPrice().toPlainString()).isEqualTo("0.00002834");
        }

        @Test
        @DisplayName("SHIB micro-price ask 0.00002835 passes through without precision loss")
        void transform_shibAskPrice_exactPrecision() {
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            assertThat(result.getBestAskPrice()).isEqualByComparingTo(SHIB_ASK);
            assertThat(result.getBestAskPrice().toPlainString()).isEqualTo("0.00002835");
        }

        @Test
        @DisplayName("Spread between SHIB bid/ask is exactly 0.00000001 (1 satoshi)")
        void transform_shibSpread_exactOneSatoshi() {
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.BINANCE), START_NANOS).orElseThrow();
            BigDecimal spread = result.getBestAskPrice().subtract(result.getBestBidPrice());
            assertThat(spread).isEqualByComparingTo(new BigDecimal("0.00000001"));
        }
    }

    @Nested
    @DisplayName("Wrong Exchange Guard")
    class WrongExchangeTests {

        @Test
        @DisplayName("Returns empty for BYBIT tick")
        void transform_bybitTick_returnsEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS)).isEmpty();
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
        @DisplayName("Returns empty when bestBidPrice is null")
        void transform_nullBidPrice_returnsEmpty() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(BTC_USDT)
                    .bestBidPrice(null)
                    .bestAskPrice(BTC_ASK)
                    .bestBidQuantity(BTC_BID_QTY)
                    .bestAskQuantity(BTC_ASK_QTY)
                    .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                    .receivedTimestamp(FIXED_RECEIVED_NANOS)
                    .processedTimestamp(FIXED_PROCESSED_NANOS)
                    .build();
            assertThat(transformer.transform(tick, START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Returns empty when bestAskPrice is null")
        void transform_nullAskPrice_returnsEmpty() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(BTC_USDT)
                    .bestBidPrice(BTC_BID)
                    .bestAskPrice(null)
                    .bestBidQuantity(BTC_BID_QTY)
                    .bestAskQuantity(BTC_ASK_QTY)
                    .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                    .receivedTimestamp(FIXED_RECEIVED_NANOS)
                    .processedTimestamp(FIXED_PROCESSED_NANOS)
                    .build();
            assertThat(transformer.transform(tick, START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Returns empty when tradingPair is null")
        void transform_nullTradingPair_returnsEmpty() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(null)
                    .bestBidPrice(BTC_BID)
                    .bestAskPrice(BTC_ASK)
                    .bestBidQuantity(BTC_BID_QTY)
                    .bestAskQuantity(BTC_ASK_QTY)
                    .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                    .receivedTimestamp(FIXED_RECEIVED_NANOS)
                    .processedTimestamp(FIXED_PROCESSED_NANOS)
                    .build();
            assertThat(transformer.transform(tick, START_NANOS)).isEmpty();
        }
    }
}
