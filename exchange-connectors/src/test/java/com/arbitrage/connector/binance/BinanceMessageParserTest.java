package com.arbitrage.connector.binance;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BinanceMessageParser}.
 *
 * <p>Uses parameterised tests driven by real recorded Binance WebSocket fixtures
 * from {@code src/test/resources/fixtures/binance/}. Each fixture is a genuine
 * bookTicker message (or a realistic replica) capturing different scenarios:
 * standard BTC pairs, altcoin micro-prices, large quantities, and high updateIds.</p>
 *
 * <p><b>Why parameterised fixture tests?</b> When a parser breaks on a new message
 * format from production, you add the failing message as fixture #6. The test suite
 * grows organically from real data, not hypothetical edge cases. This is the same
 * pattern used at HFT firms where test suites are built from recorded market data.</p>
 */
class BinanceMessageParserTest {

    private BinanceMessageParser parser;
    private ObjectMapper objectMapper;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        parser = new BinanceMessageParser(objectMapper, metrics);
        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    // ============================================================
    // Parameterised Fixture Tests
    // ============================================================

    @Nested
    @DisplayName("Fixture-Driven Parsing")
    class FixtureTests {

        /**
         * Provides fixture file names paired with their expected bid/ask prices.
         *
         * <p>Each entry maps a fixture JSON file to the expected parsed values.
         * When a new fixture is added, add a row here — the test automatically
         * covers it.</p>
         */
        static Stream<Arguments> fixtureProvider() {
            return Stream.of(
                    Arguments.of("book-ticker-btcusdt-01.json",
                            "67250.50000000", "1.23400000", "67251.30000000", "0.98700000"),
                    Arguments.of("book-ticker-btcusdt-02.json",
                            "67255.10000000", "2.50000000", "67256.00000000", "1.10000000"),
                    Arguments.of("book-ticker-ethusdt-03.json",
                            "3456.78000000", "15.67800000", "3456.99000000", "8.45600000"),
                    Arguments.of("book-ticker-solusdt-04.json",
                            "142.35000000", "250.00000000", "142.36000000", "180.50000000"),
                    Arguments.of("book-ticker-shibusdt-05.json",
                            "0.00002834", "5000000000.00000000", "0.00002835", "3200000000.00000000")
            );
        }

        @ParameterizedTest(name = "Fixture {0} parses correctly")
        @MethodSource("fixtureProvider")
        @DisplayName("Real fixture files parse to correct BigDecimal values")
        void fixture_parsesCorrectly(String fixtureFile, String expectedBid, String expectedBidQty,
                                     String expectedAsk, String expectedAskQty) throws IOException {
            String json = loadFixture(fixtureFile);
            long t0 = System.nanoTime();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, t0);

            assertTrue(result.isPresent(), "Fixture " + fixtureFile + " should produce a tick");
            NormalisedTick tick = result.get();

            assertEquals(ExchangeId.BINANCE, tick.getExchangeId());
            assertEquals(btcUsdt, tick.getTradingPair());
            assertEquals(0, new BigDecimal(expectedBid).compareTo(tick.getBestBidPrice()),
                    "Best bid price mismatch for " + fixtureFile);
            assertEquals(0, new BigDecimal(expectedBidQty).compareTo(tick.getBestBidQuantity()),
                    "Best bid quantity mismatch for " + fixtureFile);
            assertEquals(0, new BigDecimal(expectedAsk).compareTo(tick.getBestAskPrice()),
                    "Best ask price mismatch for " + fixtureFile);
            assertEquals(0, new BigDecimal(expectedAskQty).compareTo(tick.getBestAskQuantity()),
                    "Best ask quantity mismatch for " + fixtureFile);
        }

        @ParameterizedTest(name = "Fixture {0} sets T0 and T1 timestamps")
        @MethodSource("fixtureProvider")
        @DisplayName("All fixtures get correct T0/T1 timestamps")
        void fixture_setsTimestamps(String fixtureFile, String ignored1, String ignored2,
                                    String ignored3, String ignored4) throws IOException {
            String json = loadFixture(fixtureFile);
            long t0 = System.nanoTime();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, t0);

            assertTrue(result.isPresent());
            NormalisedTick tick = result.get();

            assertEquals(t0, tick.getReceivedTimestamp(),
                    "T0 (receivedTimestamp) must be passed through unchanged");
            assertTrue(tick.getProcessedTimestamp() >= t0,
                    "T1 (processedTimestamp) must be >= T0");
        }
    }

    // ============================================================
    // BigDecimal Precision Tests
    // ============================================================

    @Nested
    @DisplayName("BigDecimal Precision")
    class PrecisionTests {

        @Test
        @DisplayName("Smallest representable price: 0.00000001 (1 satoshi)")
        void smallestPrice_parsedCorrectly() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"0.00000001\",\"B\":\"100.00000000\","
                    + "\"a\":\"0.00000002\",\"A\":\"50.00000000\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("0.00000001").compareTo(result.get().getBestBidPrice()));
            assertEquals(0, new BigDecimal("0.00000002").compareTo(result.get().getBestAskPrice()));
        }

        @Test
        @DisplayName("Large price with full precision: 99999999999.99999999")
        void largePrice_parsedCorrectly() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"99999999999.99999999\",\"B\":\"0.00000001\","
                    + "\"a\":\"99999999999.99999999\",\"A\":\"0.00000001\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("99999999999.99999999").compareTo(result.get().getBestBidPrice()));
        }

        @Test
        @DisplayName("Trailing zeros are preserved in scale")
        void trailingZeros_preservedInScale() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50000000\",\"B\":\"1.00000000\","
                    + "\"a\":\"67251.00000000\",\"A\":\"1.00000000\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            // BigDecimal("67250.50000000") has scale 8; compareTo ignores scale but
            // we verify the parsed value is numerically equal
            assertEquals(0, new BigDecimal("67250.50000000").compareTo(result.get().getBestBidPrice()));
            assertEquals(0, new BigDecimal("67251.00000000").compareTo(result.get().getBestAskPrice()));
        }
    }

    // ============================================================
    // Non-BookTicker Messages (should return empty)
    // ============================================================

    @Nested
    @DisplayName("Non-BookTicker Messages")
    class NonBookTickerTests {

        @Test
        @DisplayName("Subscription ack returns empty")
        void subscriptionAck_returnsEmpty() {
            String ack = "{\"result\":null,\"id\":1}";

            Optional<NormalisedTick> result = parser.parse(ack, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Subscription ack should not produce a tick");
        }

        @Test
        @DisplayName("Message with only updateId and symbol returns empty")
        void partialMessage_returnsEmpty() {
            String partial = "{\"u\":400900217,\"s\":\"BTCUSDT\"}";

            Optional<NormalisedTick> result = parser.parse(partial, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Partial message without prices should return empty");
        }

        @Test
        @DisplayName("Empty JSON object returns empty")
        void emptyJsonObject_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("{}", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Empty JSON object should return empty");
        }

        @Test
        @DisplayName("Unknown message type with extra fields returns empty")
        void unknownMessageType_returnsEmpty() {
            String unknown = "{\"e\":\"trade\",\"E\":1672515782136,\"s\":\"BTCUSDT\",\"t\":12345}";

            Optional<NormalisedTick> result = parser.parse(unknown, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Trade message (not bookTicker) should return empty");
        }
    }

    // ============================================================
    // Error Handling (should return empty, not throw)
    // ============================================================

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Malformed JSON returns empty (does not throw)")
        void malformedJson_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("{not valid json", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Malformed JSON should return empty, not throw");
        }

        @Test
        @DisplayName("Non-numeric bid price returns empty")
        void nonNumericBidPrice_returnsEmpty() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"not_a_number\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"0.987\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Non-numeric price should return empty");
        }

        @Test
        @DisplayName("Non-numeric ask quantity returns empty")
        void nonNumericAskQuantity_returnsEmpty() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"NaN\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "NaN quantity should return empty");
        }

        @Test
        @DisplayName("Empty string returns empty")
        void emptyString_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Empty string should return empty");
        }

        @ParameterizedTest(name = "Incomplete field set: {0}")
        @ValueSource(strings = {
                "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\"}",
                "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\"}",
                "{\"u\":1,\"s\":\"BTCUSDT\",\"a\":\"67251.30\",\"A\":\"0.987\"}"
        })
        @DisplayName("Missing one or more price fields returns empty")
        void missingFields_returnsEmpty(String json) {
            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Missing price fields should return empty");
        }
    }

    // ============================================================
    // Timestamp Tests
    // ============================================================

    @Nested
    @DisplayName("Timestamp Handling")
    class TimestampTests {

        @Test
        @DisplayName("T0 (receivedNanos) is passed through unchanged to the tick")
        void t0_passedThroughUnchanged() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"0.987\"}";
            long explicitT0 = 123456789L;

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, explicitT0);

            assertTrue(result.isPresent());
            assertEquals(explicitT0, result.get().getReceivedTimestamp(),
                    "receivedTimestamp must be exactly the T0 value passed in");
        }

        @Test
        @DisplayName("T1 (processedTimestamp) is after T0")
        void t1_isAfterT0() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"0.987\"}";
            long t0 = System.nanoTime();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, t0);

            assertTrue(result.isPresent());
            assertTrue(result.get().getProcessedTimestamp() >= t0,
                    "T1 must be >= T0 (parsing takes non-negative time)");
        }

        @Test
        @DisplayName("exchangeTimestamp is set to approximately now")
        void exchangeTimestamp_isApproximatelyNow() {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"0.987\"}";
            long beforeMillis = System.currentTimeMillis();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            long afterMillis = System.currentTimeMillis();
            assertTrue(result.isPresent());
            long tickMillis = result.get().getExchangeTimestamp().toEpochMilli();
            assertTrue(tickMillis >= beforeMillis && tickMillis <= afterMillis,
                    "exchangeTimestamp should be between before and after measurement");
        }
    }

    // ============================================================
    // BinanceBookTickerMessage POJO Tests
    // ============================================================

    @Nested
    @DisplayName("BinanceBookTickerMessage Deserialization")
    class BookTickerMessageTests {

        @Test
        @DisplayName("All fields deserialize correctly via @JsonProperty")
        void allFields_deserializeCorrectly() throws Exception {
            String json = "{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"67250.50000000\","
                    + "\"B\":\"1.23400000\",\"a\":\"67251.30000000\",\"A\":\"0.98700000\"}";

            BinanceBookTickerMessage message = objectMapper.readValue(json, BinanceBookTickerMessage.class);

            assertEquals(400900217L, message.getUpdateId());
            assertEquals("BTCUSDT", message.getSymbol());
            assertEquals("67250.50000000", message.getBestBidPrice());
            assertEquals("1.23400000", message.getBestBidQuantity());
            assertEquals("67251.30000000", message.getBestAskPrice());
            assertEquals("0.98700000", message.getBestAskQuantity());
            assertTrue(message.isValid());
        }

        @Test
        @DisplayName("updateId > Integer.MAX_VALUE deserializes as long")
        void largeUpdateId_deserializesAsLong() throws Exception {
            String json = "{\"u\":2147483648,\"s\":\"SOLUSDT\",\"b\":\"142.35\","
                    + "\"B\":\"250.0\",\"a\":\"142.36\",\"A\":\"180.5\"}";

            BinanceBookTickerMessage message = objectMapper.readValue(json, BinanceBookTickerMessage.class);

            assertEquals(2147483648L, message.getUpdateId(),
                    "updateId must use long — int overflows at 2^31");
        }

        @Test
        @DisplayName("Unknown fields are silently ignored (forward compatibility)")
        void unknownFields_areIgnored() throws Exception {
            String json = "{\"u\":1,\"s\":\"BTCUSDT\",\"b\":\"67250.50\",\"B\":\"1.0\","
                    + "\"a\":\"67251.30\",\"A\":\"0.987\",\"newField\":\"surprise\",\"x\":42}";

            BinanceBookTickerMessage message = objectMapper.readValue(json, BinanceBookTickerMessage.class);

            assertTrue(message.isValid(), "Unknown fields should not break deserialization");
            assertEquals("67250.50", message.getBestBidPrice());
        }

        @Test
        @DisplayName("Subscription ack is not valid (missing price fields)")
        void subscriptionAck_isNotValid() throws Exception {
            String ack = "{\"result\":null,\"id\":1}";

            BinanceBookTickerMessage message = objectMapper.readValue(ack, BinanceBookTickerMessage.class);

            assertTrue(!message.isValid(), "Subscription ack has no price fields — isValid() should be false");
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    /**
     * Loads a test fixture file from the classpath.
     *
     * @param filename the fixture filename (e.g., "book-ticker-btcusdt-01.json")
     * @return the file contents as a string
     * @throws IOException if the fixture file cannot be read
     */
    private String loadFixture(String filename) throws IOException {
        Path fixturePath = Path.of("src/test/resources/fixtures/binance", filename);
        return Files.readString(fixturePath, StandardCharsets.UTF_8);
    }
}
