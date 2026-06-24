package com.arbitrage.connector.bybit;

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
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BybitMessageParser}.
 *
 * <p>Uses parameterised tests driven by real Bybit WebSocket fixtures from
 * {@code src/test/resources/fixtures/bybit/}. Each fixture is a realistic {@code orderbook.1}
 * message capturing different scenarios: BTC snapshots, delta updates, altcoin mid-prices,
 * micro-prices with massive quantities.</p>
 *
 * <p><b>Why orderbook.1 format:</b> The tickers stream snapshot does NOT include bid/ask prices.
 * The orderbook.1 stream always sends best bid and ask on every update (snapshot + delta).</p>
 *
 * <p><b>Key difference from BinanceMessageParserTest:</b> Bybit's exchange timestamp
 * comes from the server {@code ts} field (epoch milliseconds), not {@code Instant.now()}.
 * The timestamp tests verify that the parser uses the server-provided time.</p>
 */
class BybitMessageParserTest {

    private BybitMessageParser parser;
    private ObjectMapper objectMapper;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        parser = new BybitMessageParser(objectMapper, metrics);
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
         * Provides fixture file names paired with their expected bid/ask prices and quantities.
         *
         * <p>Each entry maps a fixture JSON file to the expected parsed values.
         * When a new fixture is added, add a row here — the test automatically covers it.</p>
         */
        static Stream<Arguments> fixtureProvider() {
            return Stream.of(
                    Arguments.of("ticker-btcusdt-01.json",
                            "21109.50000000", "0.50000000", "21109.60000000", "0.30000000"),
                    Arguments.of("ticker-btcusdt-02.json",
                            "21115.20000000", "1.75000000", "21115.80000000", "0.92000000"),
                    Arguments.of("ticker-ethusdt-03.json",
                            "3456.78000000", "15.67800000", "3456.99000000", "8.45600000"),
                    Arguments.of("ticker-solusdt-04.json",
                            "142.35000000", "250.00000000", "142.36000000", "180.50000000"),
                    Arguments.of("ticker-shibusdt-05.json",
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

            assertEquals(ExchangeId.BYBIT, tick.getExchangeId());
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
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"0.00000001\",\"100.00000000\"]],"
                    + "\"a\":[[\"0.00000002\",\"50.00000000\"]],\"u\":1,\"seq\":1}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("0.00000001").compareTo(result.get().getBestBidPrice()));
            assertEquals(0, new BigDecimal("0.00000002").compareTo(result.get().getBestAskPrice()));
        }

        @Test
        @DisplayName("Large price with full precision: 99999999999.99999999")
        void largePrice_parsedCorrectly() {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"99999999999.99999999\",\"0.00000001\"]],"
                    + "\"a\":[[\"99999999999.99999999\",\"0.00000001\"]],\"u\":1,\"seq\":1}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("99999999999.99999999").compareTo(result.get().getBestBidPrice()));
        }

        @Test
        @DisplayName("Trailing zeros are preserved in scale")
        void trailingZeros_preservedInScale() {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"67250.50000000\",\"1.00000000\"]],"
                    + "\"a\":[[\"67251.00000000\",\"1.00000000\"]],\"u\":1,\"seq\":1}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("67250.50000000").compareTo(result.get().getBestBidPrice()));
            assertEquals(0, new BigDecimal("67251.00000000").compareTo(result.get().getBestAskPrice()));
        }
    }

    // ============================================================
    // Non-Ticker Messages (should return empty)
    // ============================================================

    @Nested
    @DisplayName("Non-Ticker Messages")
    class NonTickerTests {

        @Test
        @DisplayName("Subscription ack returns empty")
        void subscriptionAck_returnsEmpty() {
            String ack = "{\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"abc123\","
                    + "\"req_id\":\"1\",\"op\":\"subscribe\"}";

            Optional<NormalisedTick> result = parser.parse(ack, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Subscription ack should not produce a tick");
        }

        @Test
        @DisplayName("Pong message returns empty")
        void pongMessage_returnsEmpty() {
            String pong = "{\"success\":true,\"ret_msg\":\"pong\",\"conn_id\":\"abc123\","
                    + "\"req_id\":\"heartbeat\",\"op\":\"pong\"}";

            Optional<NormalisedTick> result = parser.parse(pong, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Pong message should not produce a tick");
        }

        @Test
        @DisplayName("Partial data object (missing bid/ask arrays) returns empty")
        void partialMessage_returnsEmpty() {
            String partial = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\"}}";

            Optional<NormalisedTick> result = parser.parse(partial, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Partial data without bid/ask arrays should return empty");
        }

        @Test
        @DisplayName("Empty JSON object returns empty")
        void emptyJsonObject_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("{}", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Empty JSON object should return empty");
        }

        @Test
        @DisplayName("Delta message type parses correctly (same as snapshot)")
        void deltaMessage_parsesCorrectly() throws IOException {
            String json = loadFixture("ticker-btcusdt-02.json");

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent(), "Delta messages should be parsed just like snapshots");
            assertEquals(ExchangeId.BYBIT, result.get().getExchangeId());
            assertEquals(0, new BigDecimal("21115.20000000").compareTo(result.get().getBestBidPrice()));
        }

        @Test
        @DisplayName("Message with null data object returns empty")
        void nullDataObject_returnsEmpty() {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Null data object should return empty");
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
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"not_a_number\",\"1.0\"]],"
                    + "\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Non-numeric price should return empty");
        }

        @Test
        @DisplayName("Non-numeric ask size returns empty")
        void nonNumericAskSize_returnsEmpty() {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],"
                    + "\"a\":[[\"21109.60\",\"NaN\"]],\"u\":1,\"seq\":1}}";

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
                "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1,\"type\":\"snapshot\",\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\"]],\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}",
                "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1,\"type\":\"snapshot\",\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],\"u\":1,\"seq\":1}}",
                "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1,\"type\":\"snapshot\",\"data\":{\"s\":\"BTCUSDT\",\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}"
        })
        @DisplayName("Missing one or more bid/ask fields returns empty")
        void missingFields_returnsEmpty(String json) {
            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Missing bid/ask fields should return empty");
        }
    }

    // ============================================================
    // Delta-Merging Tests — Stateful Single-Side Handling
    // ============================================================

    @Nested
    @DisplayName("Delta Merging — Stateful Single-Side Handling")
    class DeltaMergingTests {

        private static final String SNAPSHOT = "{\"topic\":\"orderbook.1.BTCUSDT\","
                + "\"ts\":1673853746003,\"type\":\"snapshot\","
                + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],"
                + "\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}";

        @Test
        @DisplayName("Bid-only delta emits tick using stored ask from prior snapshot")
        void bidOnlyDelta_mergesWithStoredAsk() {
            parser.parse(SNAPSHOT, btcUsdt, System.nanoTime());

            String bidOnlyDelta = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746100,"
                    + "\"type\":\"delta\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21110.00\",\"1.0\"]],"
                    + "\"a\":[],\"u\":2,\"seq\":2}}";

            Optional<NormalisedTick> result = parser.parse(bidOnlyDelta, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent(), "Bid-only delta must emit a tick (stored ask available)");
            assertEquals(0, new BigDecimal("21110.00").compareTo(result.get().getBestBidPrice()),
                    "Bid price must come from the delta");
            assertEquals(0, new BigDecimal("21109.60").compareTo(result.get().getBestAskPrice()),
                    "Ask price must be carried over from the prior snapshot");
        }

        @Test
        @DisplayName("Ask-only delta emits tick using stored bid from prior snapshot")
        void askOnlyDelta_mergesWithStoredBid() {
            parser.parse(SNAPSHOT, btcUsdt, System.nanoTime());

            String askOnlyDelta = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746100,"
                    + "\"type\":\"delta\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[],"
                    + "\"a\":[[\"21111.00\",\"2.0\"]],\"u\":2,\"seq\":2}}";

            Optional<NormalisedTick> result = parser.parse(askOnlyDelta, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent(), "Ask-only delta must emit a tick (stored bid available)");
            assertEquals(0, new BigDecimal("21111.00").compareTo(result.get().getBestAskPrice()),
                    "Ask price must come from the delta");
            assertEquals(0, new BigDecimal("21109.50").compareTo(result.get().getBestBidPrice()),
                    "Bid price must be carried over from the prior snapshot");
        }

        @Test
        @DisplayName("Bid-only delta before any snapshot returns empty (awaiting_snapshot)")
        void bidOnlyDelta_beforeSnapshot_returnsEmpty() {
            String bidOnlyDelta = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746100,"
                    + "\"type\":\"delta\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21110.00\",\"1.0\"]],"
                    + "\"a\":[],\"u\":2,\"seq\":2}}";

            Optional<NormalisedTick> result = parser.parse(bidOnlyDelta, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(),
                    "Bid-only delta with no prior snapshot must return empty (ask side unknown)");
        }

        @Test
        @DisplayName("Sequential single-side deltas accumulate state correctly")
        void sequentialSingleSideDeltas_accumulateState() {
            parser.parse(SNAPSHOT, btcUsdt, System.nanoTime());

            // Delta 1: bid moves up (ask unchanged)
            String delta1 = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746100,"
                    + "\"type\":\"delta\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21110.00\",\"0.8\"]],"
                    + "\"a\":[],\"u\":2,\"seq\":2}}";
            parser.parse(delta1, btcUsdt, System.nanoTime());

            // Delta 2: ask moves up (bid unchanged)
            String delta2 = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746200,"
                    + "\"type\":\"delta\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[],"
                    + "\"a\":[[\"21112.00\",\"1.5\"]],\"u\":3,\"seq\":3}}";
            Optional<NormalisedTick> result = parser.parse(delta2, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("21110.00").compareTo(result.get().getBestBidPrice()),
                    "Bid must reflect delta1");
            assertEquals(0, new BigDecimal("21112.00").compareTo(result.get().getBestAskPrice()),
                    "Ask must reflect delta2");
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
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],"
                    + "\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}";
            long explicitT0 = 123456789L;

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, explicitT0);

            assertTrue(result.isPresent());
            assertEquals(explicitT0, result.get().getReceivedTimestamp(),
                    "receivedTimestamp must be exactly the T0 value passed in");
        }

        @Test
        @DisplayName("T1 (processedTimestamp) is after T0")
        void t1_isAfterT0() {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],"
                    + "\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}";
            long t0 = System.nanoTime();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, t0);

            assertTrue(result.isPresent());
            assertTrue(result.get().getProcessedTimestamp() >= t0,
                    "T1 must be >= T0 (parsing takes non-negative time)");
        }

        @Test
        @DisplayName("exchangeTimestamp uses server ts field (not Instant.now())")
        void exchangeTimestamp_usesServerTimestamp() {
            long serverTimestampMs = 1673853746003L;
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":" + serverTimestampMs
                    + ",\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.5\"]],"
                    + "\"a\":[[\"21109.60\",\"0.3\"]],\"u\":1,\"seq\":1}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            Instant expectedTimestamp = Instant.ofEpochMilli(serverTimestampMs);
            assertEquals(expectedTimestamp, result.get().getExchangeTimestamp(),
                    "exchangeTimestamp must match server ts field, not Instant.now()");
        }
    }

    // ============================================================
    // BybitTickerMessage POJO Tests
    // ============================================================

    @Nested
    @DisplayName("BybitTickerMessage Deserialization")
    class TickerMessageTests {

        @Test
        @DisplayName("All fields deserialize correctly")
        void allFields_deserializeCorrectly() throws Exception {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.50\"]],"
                    + "\"a\":[[\"21109.60\",\"0.30\"]],\"u\":18521288,\"seq\":7961638724}}";

            BybitTickerMessage message = objectMapper.readValue(json, BybitTickerMessage.class);

            assertEquals("orderbook.1.BTCUSDT", message.getTopic());
            assertEquals(1673853746003L, message.getTs());
            assertEquals("snapshot", message.getType());
            assertTrue(message.isTickerMessage());
            assertFalse(message.isPong());
            assertFalse(message.isSubscriptionAck());

            BybitTickerMessage.OrderbookData data = message.getData();
            assertNotNull(data);
            assertEquals("BTCUSDT", data.getSymbol());
            assertEquals("21109.50", data.bestBidPrice());
            assertEquals("0.50", data.bestBidSize());
            assertEquals("21109.60", data.bestAskPrice());
            assertEquals("0.30", data.bestAskSize());
            assertEquals(18521288L, data.getUpdateId());
            assertEquals(7961638724L, data.getSeq());
        }

        @Test
        @DisplayName("Unknown fields are silently ignored (forward compatibility)")
        void unknownFields_areIgnored() throws Exception {
            String json = "{\"topic\":\"orderbook.1.BTCUSDT\",\"ts\":1673853746003,\"type\":\"snapshot\","
                    + "\"newField\":\"surprise\",\"x\":42,"
                    + "\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"21109.50\",\"0.50\"]],"
                    + "\"a\":[[\"21109.60\",\"0.30\"]],\"u\":1,\"seq\":1,"
                    + "\"extraOrderbookField\":\"ignored\"}}";

            BybitTickerMessage message = objectMapper.readValue(json, BybitTickerMessage.class);

            assertTrue(message.isTickerMessage(), "Unknown fields should not break deserialization");
            assertEquals("21109.50", message.getData().bestBidPrice());
        }

        @Test
        @DisplayName("Subscription ack is not a ticker message")
        void subscriptionAck_isNotTickerMessage() throws Exception {
            String ack = "{\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"abc123\","
                    + "\"req_id\":\"1\",\"op\":\"subscribe\"}";

            BybitTickerMessage message = objectMapper.readValue(ack, BybitTickerMessage.class);

            assertFalse(message.isTickerMessage(), "Subscription ack has no data — isTickerMessage() should be false");
            assertTrue(message.isSubscriptionAck());
            assertFalse(message.isPong());
        }

        @Test
        @DisplayName("Pong message is identified correctly")
        void pongMessage_identifiedCorrectly() throws Exception {
            String pong = "{\"success\":true,\"ret_msg\":\"pong\",\"conn_id\":\"abc123\","
                    + "\"req_id\":\"heartbeat\",\"op\":\"pong\"}";

            BybitTickerMessage message = objectMapper.readValue(pong, BybitTickerMessage.class);

            assertTrue(message.isPong());
            assertFalse(message.isSubscriptionAck());
            assertFalse(message.isTickerMessage());
        }

        @Test
        @DisplayName("Nested data fields map correctly via @JsonProperty compact names")
        void nestedDataFields_mapCorrectly() throws Exception {
            String json = "{\"topic\":\"orderbook.1.ETHUSDT\",\"ts\":1673853748200,\"type\":\"snapshot\","
                    + "\"data\":{\"s\":\"ETHUSDT\",\"b\":[[\"3456.78\",\"15.678\"]],"
                    + "\"a\":[[\"3456.99\",\"8.456\"]],\"u\":2,\"seq\":2}}";

            BybitTickerMessage message = objectMapper.readValue(json, BybitTickerMessage.class);

            assertEquals("ETHUSDT", message.getData().getSymbol());
            assertEquals("3456.78", message.getData().bestBidPrice());
            assertEquals("15.678", message.getData().bestBidSize());
            assertEquals("3456.99", message.getData().bestAskPrice());
            assertEquals("8.456", message.getData().bestAskSize());
            assertTrue(message.getData().isValid());
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    /**
     * Loads a test fixture file from the classpath.
     *
     * @param filename the fixture filename (e.g., "ticker-btcusdt-01.json")
     * @return the file contents as a string
     * @throws IOException if the fixture file cannot be read
     */
    private String loadFixture(String filename) throws IOException {
        Path fixturePath = Path.of("src/test/resources/fixtures/bybit", filename);
        return Files.readString(fixturePath, StandardCharsets.UTF_8);
    }
}
