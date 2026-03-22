package com.arbitrage.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link NormalisedTick} — the universal tick schema on the hot path.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Builder creates a valid, fully-populated tick</li>
 *   <li>BigDecimal precision is preserved at 8 decimal places (crypto precision)</li>
 *   <li>Immutability (no setters available — enforced by {@code @Value})</li>
 *   <li>Nanosecond timestamp fields carry monotonic clock values correctly</li>
 * </ul>
 *
 * <p><b>Why BigDecimal precision tests matter:</b> In crypto, prices have 8 decimal
 * places and spreads can be as small as 0.01%. A rounding error of {@code 1e-10}
 * can flip a comparison from "profitable" to "not profitable." These tests ensure
 * that BigDecimal values round-trip through the builder without precision loss.
 */
class NormalisedTickTest {

    private static final ExchangeId TEST_EXCHANGE = ExchangeId.BINANCE;
    private static final TradingPair TEST_PAIR = TradingPair.fromSymbol("BTC-USDT");

    /**
     * Creates a fully-populated test tick with realistic BTC-USDT values.
     */
    private NormalisedTick createTestTick() {
        return NormalisedTick.builder()
                .exchangeId(TEST_EXCHANGE)
                .tradingPair(TEST_PAIR)
                .bestBidPrice(new BigDecimal("67250.50000000"))
                .bestAskPrice(new BigDecimal("67251.30000000"))
                .bestBidQuantity(new BigDecimal("1.23456789"))
                .bestAskQuantity(new BigDecimal("0.98765432"))
                .exchangeTimestamp(Instant.parse("2026-03-22T10:15:30.123Z"))
                .receivedTimestamp(123456789000000L)
                .processedTimestamp(123456789500000L)
                .build();
    }

    @Nested
    @DisplayName("Builder and field access")
    class BuilderTests {

        @Test
        @DisplayName("builder creates tick with all fields populated")
        void builderCreatesFullyPopulatedTick() {
            NormalisedTick tick = createTestTick();

            assertEquals(ExchangeId.BINANCE, tick.getExchangeId());
            assertEquals(TEST_PAIR, tick.getTradingPair());
            assertNotNull(tick.getBestBidPrice());
            assertNotNull(tick.getBestAskPrice());
            assertNotNull(tick.getBestBidQuantity());
            assertNotNull(tick.getBestAskQuantity());
            assertNotNull(tick.getExchangeTimestamp());
            assertTrue(tick.getReceivedTimestamp() > 0);
            assertTrue(tick.getProcessedTimestamp() > 0);
        }

        @Test
        @DisplayName("processedTimestamp is after receivedTimestamp")
        void processedIsAfterReceived() {
            NormalisedTick tick = createTestTick();

            assertTrue(tick.getProcessedTimestamp() > tick.getReceivedTimestamp(),
                    "processedTimestamp must be after receivedTimestamp — " +
                            "normalisation takes non-zero time");
        }
    }

    @Nested
    @DisplayName("BigDecimal precision")
    class PrecisionTests {

        @Test
        @DisplayName("preserves 8 decimal places on bid price")
        void preservesBidPricePrecision() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(TEST_EXCHANGE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(new BigDecimal("67250.12345678"))
                    .bestAskPrice(BigDecimal.ONE)
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            assertEquals(new BigDecimal("67250.12345678"), tick.getBestBidPrice());
            assertEquals(8, tick.getBestBidPrice().scale(),
                    "Bid price must retain 8 decimal places (crypto precision)");
        }

        @Test
        @DisplayName("preserves 8 decimal places on ask price")
        void preservesAskPricePrecision() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(TEST_EXCHANGE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(BigDecimal.ONE)
                    .bestAskPrice(new BigDecimal("67251.87654321"))
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            assertEquals(new BigDecimal("67251.87654321"), tick.getBestAskPrice());
            assertEquals(8, tick.getBestAskPrice().scale());
        }

        @Test
        @DisplayName("preserves 8 decimal places on quantities")
        void preservesQuantityPrecision() {
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(TEST_EXCHANGE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(BigDecimal.ONE)
                    .bestAskPrice(BigDecimal.ONE)
                    .bestBidQuantity(new BigDecimal("0.00000001"))
                    .bestAskQuantity(new BigDecimal("99999999.99999999"))
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            assertEquals(new BigDecimal("0.00000001"), tick.getBestBidQuantity(),
                    "Must handle minimum satoshi-level quantity (1e-8)");
            assertEquals(new BigDecimal("99999999.99999999"), tick.getBestAskQuantity(),
                    "Must handle large quantities without precision loss");
        }

        @Test
        @DisplayName("BigDecimal comparisons use compareTo, not equals")
        void bigDecimalComparisonSemantics() {
            BigDecimal price1 = new BigDecimal("67250.50000000");
            BigDecimal price2 = new BigDecimal("67250.5");

            // BigDecimal.equals() considers scale — these are NOT equal
            assertNotEquals(price1, price2,
                    "BigDecimal.equals() considers scale — 67250.50000000 != 67250.5");

            // BigDecimal.compareTo() ignores scale — these ARE equivalent
            assertEquals(0, price1.compareTo(price2),
                    "BigDecimal.compareTo() ignores scale — use this for price comparisons");
        }
    }

    @Nested
    @DisplayName("Value equality (Lombok @Value)")
    class EqualityTests {

        @Test
        @DisplayName("two ticks with identical fields are equal")
        void identicalTicksAreEqual() {
            Instant timestamp = Instant.parse("2026-03-22T10:00:00Z");
            long receivedNanos = 100000000L;
            long processedNanos = 100500000L;

            NormalisedTick tick1 = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(new BigDecimal("67250.50000000"))
                    .bestAskPrice(new BigDecimal("67251.30000000"))
                    .bestBidQuantity(new BigDecimal("1.00000000"))
                    .bestAskQuantity(new BigDecimal("2.00000000"))
                    .exchangeTimestamp(timestamp)
                    .receivedTimestamp(receivedNanos)
                    .processedTimestamp(processedNanos)
                    .build();

            NormalisedTick tick2 = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(new BigDecimal("67250.50000000"))
                    .bestAskPrice(new BigDecimal("67251.30000000"))
                    .bestBidQuantity(new BigDecimal("1.00000000"))
                    .bestAskQuantity(new BigDecimal("2.00000000"))
                    .exchangeTimestamp(timestamp)
                    .receivedTimestamp(receivedNanos)
                    .processedTimestamp(processedNanos)
                    .build();

            assertEquals(tick1, tick2);
            assertEquals(tick1.hashCode(), tick2.hashCode());
        }

        @Test
        @DisplayName("ticks from different exchanges are not equal")
        void differentExchangesAreNotEqual() {
            NormalisedTick binanceTick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BINANCE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(new BigDecimal("67250.50000000"))
                    .bestAskPrice(new BigDecimal("67251.30000000"))
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            NormalisedTick bybitTick = NormalisedTick.builder()
                    .exchangeId(ExchangeId.BYBIT)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(new BigDecimal("67250.50000000"))
                    .bestAskPrice(new BigDecimal("67251.30000000"))
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            assertNotEquals(binanceTick, bybitTick);
        }
    }

    @Nested
    @DisplayName("Timestamp semantics")
    class TimestampTests {

        @Test
        @DisplayName("receivedTimestamp is a nanoTime value (not wall-clock)")
        void receivedTimestampIsNanoTime() {
            long beforeNanos = System.nanoTime();
            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(TEST_EXCHANGE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(BigDecimal.ONE)
                    .bestAskPrice(BigDecimal.ONE)
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(Instant.now())
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();
            long afterNanos = System.nanoTime();

            assertTrue(tick.getReceivedTimestamp() >= beforeNanos,
                    "receivedTimestamp should be captured between test start and end");
            assertTrue(tick.getReceivedTimestamp() <= afterNanos,
                    "receivedTimestamp should be captured between test start and end");
        }

        @Test
        @DisplayName("exchangeTimestamp is an Instant (wall-clock from exchange)")
        void exchangeTimestampIsInstant() {
            Instant exchangeTime = Instant.parse("2026-03-22T10:15:30.123Z");

            NormalisedTick tick = NormalisedTick.builder()
                    .exchangeId(TEST_EXCHANGE)
                    .tradingPair(TEST_PAIR)
                    .bestBidPrice(BigDecimal.ONE)
                    .bestAskPrice(BigDecimal.ONE)
                    .bestBidQuantity(BigDecimal.ONE)
                    .bestAskQuantity(BigDecimal.ONE)
                    .exchangeTimestamp(exchangeTime)
                    .receivedTimestamp(System.nanoTime())
                    .processedTimestamp(System.nanoTime())
                    .build();

            assertEquals(exchangeTime, tick.getExchangeTimestamp());
            assertEquals(123, tick.getExchangeTimestamp().toEpochMilli() % 1000,
                    "Exchange timestamp should preserve millisecond precision");
        }
    }
}
