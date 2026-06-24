package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.arbitrage.normalisation.transformer.TickTransformerTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KuCoinTickTransformer}.
 *
 * <p>KuCoin-specific note: KuCoin is the only exchange whose native symbol format
 * ({@code BTC-USDT}) matches our canonical format exactly. No symbol conversion occurs
 * anywhere in the pipeline for KuCoin. These tests verify that the transformer correctly
 * processes pre-canonicalised ticks.
 */
class KuCoinTickTransformerTest {

    private KuCoinTickTransformer transformer;
    private static final long START_NANOS = 999_000_000L;

    @BeforeEach
    void setUp() {
        transformer = new KuCoinTickTransformer();
    }

    @Test
    @DisplayName("supports() returns KUCOIN")
    void supports_returnsKuCoin() {
        assertThat(transformer.supports()).isEqualTo(ExchangeId.KUCOIN);
    }

    @Nested
    @DisplayName("Happy Path")
    class HappyPath {

        @Test
        @DisplayName("Returns non-empty Optional for a valid KuCoin tick")
        void transform_validTick_returnsNonEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS)).isPresent();
        }

        @Test
        @DisplayName("exchangeId is preserved as KUCOIN")
        void transform_preservesExchangeId() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getExchangeId()).isEqualTo(ExchangeId.KUCOIN);
        }

        @Test
        @DisplayName("tradingPair BTC-USDT is preserved (KuCoin native format equals canonical)")
        void transform_preservesTradingPair() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getTradingPair().canonicalSymbol()).isEqualTo("BTC-USDT");
        }

        @Test
        @DisplayName("bestBidPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesBidPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getBestBidPrice()).isEqualByComparingTo(BTC_BID);
            assertThat(result.getBestBidPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("bestAskPrice passes through with exact BigDecimal precision (8 decimal places)")
        void transform_preservesAskPrecision() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getBestAskPrice()).isEqualByComparingTo(BTC_ASK);
            assertThat(result.getBestAskPrice().scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("exchangeTimestamp is preserved — KuCoin provides real server timestamp")
        void transform_preservesExchangeTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getExchangeTimestamp()).isEqualTo(FIXED_EXCHANGE_TIMESTAMP);
        }

        @Test
        @DisplayName("receivedTimestamp (T0) is preserved — staleness anchor must not change")
        void transform_preservesReceivedTimestamp() {
            NormalisedTick result = transformer.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getReceivedTimestamp()).isEqualTo(FIXED_RECEIVED_NANOS);
        }

        @Test
        @DisplayName("processedTimestamp (T4) is updated to a value after the inbound processedTimestamp")
        void transform_updatesProcessedTimestamp() {
            NormalisedTick inbound = buildBtcTick(ExchangeId.KUCOIN);
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
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getBestBidPrice().toPlainString()).isEqualTo("0.00002834");
        }

        @Test
        @DisplayName("SHIB micro-price ask 0.00002835 passes through without precision loss")
        void transform_shibAskPrice_exactPrecision() {
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            assertThat(result.getBestAskPrice().toPlainString()).isEqualTo("0.00002835");
        }

        @Test
        @DisplayName("Spread between SHIB bid/ask is exactly 0.00000001")
        void transform_shibSpread_exactOneSatoshi() {
            NormalisedTick result = transformer.transform(buildShibTick(ExchangeId.KUCOIN), START_NANOS).orElseThrow();
            BigDecimal spread = result.getBestAskPrice().subtract(result.getBestBidPrice());
            assertThat(spread).isEqualByComparingTo(new BigDecimal("0.00000001"));
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
        @DisplayName("Returns empty for BYBIT tick")
        void transform_bybitTick_returnsEmpty() {
            assertThat(transformer.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS)).isEmpty();
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
        @DisplayName("Returns empty when exchangeTimestamp is null")
        void transform_nullExchangeTimestamp_returnsEmpty() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.KUCOIN)
                    .tradingPair(BTC_USDT)
                    .bestBidPrice(BTC_BID)
                    .bestAskPrice(BTC_ASK)
                    .bestBidQuantity(BTC_BID_QTY)
                    .bestAskQuantity(BTC_ASK_QTY)
                    .exchangeTimestamp(null)
                    .receivedTimestamp(FIXED_RECEIVED_NANOS)
                    .processedTimestamp(FIXED_PROCESSED_NANOS)
                    .build();
            assertThat(transformer.transform(tick, START_NANOS)).isEmpty();
        }
    }
}
