package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.arbitrage.normalisation.transformer.TickTransformerTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TickTransformerFactory}.
 *
 * <p>Verifies routing logic: each exchange's ticks reach the correct transformer,
 * unknown exchanges return empty, and the factory auto-discovers all registered
 * transformers at construction time.
 */
class TickTransformerFactoryTest {

    private TickTransformerFactory factory;

    @BeforeEach
    void setUp() {
        // Simulates Spring injecting all TickTransformer beans as a list
        factory = new TickTransformerFactory(List.of(
                new BinanceTickTransformer(),
                new BybitTickTransformer(),
                new KuCoinTickTransformer()
        ));
    }

    private static final long START_NANOS = 999_000_000L;

    @Nested
    @DisplayName("Routing")
    class RoutingTests {

        @Test
        @DisplayName("BINANCE tick is routed to BinanceTickTransformer")
        void transform_binanceTick_routesToBinanceTransformer() {
            Optional<NormalisedTick> result = factory.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS);
            assertThat(result).isPresent();
            assertThat(result.get().getExchangeId()).isEqualTo(ExchangeId.BINANCE);
        }

        @Test
        @DisplayName("BYBIT tick is routed to BybitTickTransformer")
        void transform_bybitTick_routesToBybitTransformer() {
            Optional<NormalisedTick> result = factory.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS);
            assertThat(result).isPresent();
            assertThat(result.get().getExchangeId()).isEqualTo(ExchangeId.BYBIT);
        }

        @Test
        @DisplayName("KUCOIN tick is routed to KuCoinTickTransformer")
        void transform_kucoinTick_routesToKuCoinTransformer() {
            Optional<NormalisedTick> result = factory.transform(buildBtcTick(ExchangeId.KUCOIN), START_NANOS);
            assertThat(result).isPresent();
            assertThat(result.get().getExchangeId()).isEqualTo(ExchangeId.KUCOIN);
        }

        @Test
        @DisplayName("Each exchange transformer preserves receivedTimestamp (T0)")
        void transform_allExchanges_preserveReceivedTimestamp() {
            for (ExchangeId exchangeId : ExchangeId.values()) {
                NormalisedTick result = factory.transform(buildBtcTick(exchangeId), START_NANOS).orElseThrow();
                assertThat(result.getReceivedTimestamp())
                        .as("receivedTimestamp must be preserved for %s", exchangeId)
                        .isEqualTo(FIXED_RECEIVED_NANOS);
            }
        }

        @Test
        @DisplayName("Each exchange transformer updates processedTimestamp (T4)")
        void transform_allExchanges_updateProcessedTimestamp() {
            for (ExchangeId exchangeId : ExchangeId.values()) {
                NormalisedTick inbound = buildBtcTick(exchangeId);
                NormalisedTick result = factory.transform(inbound, START_NANOS).orElseThrow();
                assertThat(result.getProcessedTimestamp())
                        .as("processedTimestamp must be updated for %s", exchangeId)
                        .isGreaterThan(inbound.getProcessedTimestamp());
            }
        }
    }

    @Nested
    @DisplayName("supports()")
    class SupportsTests {

        @Test
        @DisplayName("supports(BINANCE) returns true")
        void supports_binance_returnsTrue() {
            assertThat(factory.supports(ExchangeId.BINANCE)).isTrue();
        }

        @Test
        @DisplayName("supports(BYBIT) returns true")
        void supports_bybit_returnsTrue() {
            assertThat(factory.supports(ExchangeId.BYBIT)).isTrue();
        }

        @Test
        @DisplayName("supports(KUCOIN) returns true")
        void supports_kucoin_returnsTrue() {
            assertThat(factory.supports(ExchangeId.KUCOIN)).isTrue();
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Returns empty for null tick")
        void transform_nullTick_returnsEmpty() {
            assertThat(factory.transform(null, START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Empty transformer list returns empty for any tick")
        void transform_noTransformers_returnsEmpty() {
            TickTransformerFactory emptyFactory = new TickTransformerFactory(List.of());
            assertThat(emptyFactory.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS)).isEmpty();
        }

        @Test
        @DisplayName("Factory with only Binance transformer returns empty for Bybit tick")
        void transform_partialTransformers_unknownExchangeReturnsEmpty() {
            TickTransformerFactory partialFactory = new TickTransformerFactory(
                    List.of(new BinanceTickTransformer()));
            assertThat(partialFactory.transform(buildBtcTick(ExchangeId.BYBIT), START_NANOS)).isEmpty();
            assertThat(partialFactory.transform(buildBtcTick(ExchangeId.BINANCE), START_NANOS)).isPresent();
        }
    }
}
