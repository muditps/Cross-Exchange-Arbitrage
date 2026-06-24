package com.arbitrage.connector.kucoin;

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
 * Tests for {@link KuCoinMessageParser}.
 *
 * <p>Uses parameterised tests driven by real KuCoin WebSocket fixtures from
 * {@code src/test/resources/fixtures/kucoin/}. Fixtures cover BTC snapshots,
 * ETH mid-price, SOL, and a micro-price SHIB message.</p>
 *
 * <p><b>Key KuCoin-specific assertions:</b></p>
 * <ul>
 *   <li>Welcome message is skipped (not a parse error)</li>
 *   <li>Exchange timestamp comes from {@code data.time} (epoch millis), not {@code Instant.now()}</li>
 *   <li>KuCoin symbol format ({@code BTC-USDT}) matches canonical — no conversion needed</li>
 * </ul>
 */
class KuCoinMessageParserTest {

    private KuCoinMessageParser parser;
    private ObjectMapper objectMapper;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        parser = new KuCoinMessageParser(objectMapper, metrics);
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
         * Maps each fixture file to its expected bid/ask price and quantity values.
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

            assertEquals(ExchangeId.KUCOIN, tick.getExchangeId());
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
            String json = tickerJson("0.00000001", "100.00000000", "0.00000002", "50.00000000",
                    1673853746003L);

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("0.00000001").compareTo(result.get().getBestBidPrice()));
            assertEquals(0, new BigDecimal("0.00000002").compareTo(result.get().getBestAskPrice()));
        }

        @Test
        @DisplayName("Large price with full precision: 99999999999.99999999")
        void largePrice_parsedCorrectly() {
            String json = tickerJson("99999999999.99999999", "0.00000001",
                    "99999999999.99999999", "0.00000001", 1673853746003L);

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("99999999999.99999999").compareTo(result.get().getBestBidPrice()));
        }

        @Test
        @DisplayName("Massive quantity (SHIB-style): 5000000000.00000000")
        void massiveQuantity_parsedCorrectly() {
            String json = tickerJson("0.00002834", "5000000000.00000000",
                    "0.00002835", "3200000000.00000000", 1673853746003L);

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            assertEquals(0, new BigDecimal("5000000000.00000000").compareTo(result.get().getBestBidQuantity()));
        }
    }

    // ============================================================
    // Non-Ticker Messages (should return empty)
    // ============================================================

    @Nested
    @DisplayName("Non-Ticker Messages")
    class NonTickerTests {

        @Test
        @DisplayName("Welcome message returns empty (not an error)")
        void welcomeMessage_returnsEmpty() {
            String welcome = "{\"id\":\"abc123\",\"type\":\"welcome\"}";

            Optional<NormalisedTick> result = parser.parse(welcome, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Welcome message should not produce a tick");
        }

        @Test
        @DisplayName("Pong response returns empty")
        void pongMessage_returnsEmpty() {
            String pong = "{\"id\":\"ping-0\",\"type\":\"pong\"}";

            Optional<NormalisedTick> result = parser.parse(pong, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Pong should not produce a tick");
        }

        @Test
        @DisplayName("Subscription ack returns empty")
        void subscriptionAck_returnsEmpty() {
            String ack = "{\"id\":\"sub-1\",\"type\":\"ack\"}";

            Optional<NormalisedTick> result = parser.parse(ack, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Subscription ack should not produce a tick");
        }

        @Test
        @DisplayName("Message without data object returns empty")
        void messageWithoutData_returnsEmpty() {
            String json = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\",\"subject\":\"trade.ticker\"}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Message without data should return empty");
        }

        @Test
        @DisplayName("Empty JSON object returns empty")
        void emptyJsonObject_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("{}", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Empty JSON should return empty");
        }
    }

    // ============================================================
    // Error Handling Tests
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
        @DisplayName("Non-numeric bestBid returns empty")
        void nonNumericBestBid_returnsEmpty() {
            String json = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\",\"data\":{\"bestBid\":\"not_a_number\","
                    + "\"bestBidSize\":\"0.5\",\"bestAsk\":\"21109.60\",\"bestAskSize\":\"0.3\","
                    + "\"time\":1673853746003}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Non-numeric price should return empty");
        }

        @Test
        @DisplayName("Non-numeric bestAskSize returns empty")
        void nonNumericBestAskSize_returnsEmpty() {
            String json = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\",\"data\":{\"bestBid\":\"21109.50\","
                    + "\"bestBidSize\":\"0.5\",\"bestAsk\":\"21109.60\",\"bestAskSize\":\"NaN\","
                    + "\"time\":1673853746003}}";

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "NaN quantity should return empty");
        }

        @Test
        @DisplayName("Empty string returns empty")
        void emptyString_returnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("", btcUsdt, System.nanoTime());

            assertTrue(result.isEmpty(), "Empty string should return empty");
        }

        @ParameterizedTest(name = "Incomplete data fields: {0}")
        @ValueSource(strings = {
                "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\",\"subject\":\"trade.ticker\",\"data\":{\"bestBid\":\"21109.50\"}}",
                "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\",\"subject\":\"trade.ticker\",\"data\":{\"bestBid\":\"21109.50\",\"bestBidSize\":\"0.5\"}}",
                "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\",\"subject\":\"trade.ticker\",\"data\":{\"bestAsk\":\"21109.60\",\"bestAskSize\":\"0.3\"}}"
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
            String json = tickerJson("21109.50", "0.5", "21109.60", "0.3", 1673853746003L);
            long explicitT0 = 123456789L;

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, explicitT0);

            assertTrue(result.isPresent());
            assertEquals(explicitT0, result.get().getReceivedTimestamp(),
                    "receivedTimestamp must be exactly the T0 value passed in");
        }

        @Test
        @DisplayName("T1 (processedTimestamp) is >= T0")
        void t1_isAfterT0() {
            String json = tickerJson("21109.50", "0.5", "21109.60", "0.3", 1673853746003L);
            long t0 = System.nanoTime();

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, t0);

            assertTrue(result.isPresent());
            assertTrue(result.get().getProcessedTimestamp() >= t0,
                    "T1 must be >= T0 (parsing takes non-negative time)");
        }

        @Test
        @DisplayName("exchangeTimestamp uses data.time field (not Instant.now())")
        void exchangeTimestamp_usesDataTimeField() {
            long serverTimestampMs = 1673853746003L;
            String json = tickerJson("21109.50", "0.5", "21109.60", "0.3", serverTimestampMs);

            Optional<NormalisedTick> result = parser.parse(json, btcUsdt, System.nanoTime());

            assertTrue(result.isPresent());
            Instant expectedTimestamp = Instant.ofEpochMilli(serverTimestampMs);
            assertEquals(expectedTimestamp, result.get().getExchangeTimestamp(),
                    "exchangeTimestamp must match data.time, not Instant.now()");
        }
    }

    // ============================================================
    // KuCoinTickerMessage POJO Tests
    // ============================================================

    @Nested
    @DisplayName("KuCoinTickerMessage Deserialization")
    class TickerMessageTests {

        @Test
        @DisplayName("All fields deserialize correctly")
        void allFields_deserializeCorrectly() throws Exception {
            String json = tickerJson("21109.50", "0.50", "21109.60", "0.30", 1673853746003L);

            KuCoinTickerMessage message = objectMapper.readValue(json, KuCoinTickerMessage.class);

            assertEquals("message", message.getType());
            assertEquals("/market/ticker:BTC-USDT", message.getTopic());
            assertEquals("trade.ticker", message.getSubject());
            assertTrue(message.isTickerMessage());
            assertFalse(message.isPong());
            assertFalse(message.isWelcome());
            assertFalse(message.isAck());

            KuCoinTickerMessage.TickerData data = message.getData();
            assertNotNull(data);
            assertEquals("21109.50", data.getBestBid());
            assertEquals("0.50", data.getBestBidSize());
            assertEquals("21109.60", data.getBestAsk());
            assertEquals("0.30", data.getBestAskSize());
            assertEquals(1673853746003L, data.getTime());
        }

        @Test
        @DisplayName("Unknown fields are silently ignored (forward compatibility)")
        void unknownFields_areIgnored() throws Exception {
            String json = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\",\"newField\":\"surprise\","
                    + "\"data\":{\"bestBid\":\"21109.50\",\"bestBidSize\":\"0.50\","
                    + "\"bestAsk\":\"21109.60\",\"bestAskSize\":\"0.30\","
                    + "\"time\":1673853746003,\"sequence\":\"123\",\"price\":\"21110.00\"}}";

            KuCoinTickerMessage message = objectMapper.readValue(json, KuCoinTickerMessage.class);

            assertTrue(message.isTickerMessage(), "Unknown fields must not break deserialization");
            assertEquals("21109.50", message.getData().getBestBid());
        }

        @Test
        @DisplayName("Welcome message is correctly identified")
        void welcomeMessage_isIdentifiedCorrectly() throws Exception {
            String welcome = "{\"id\":\"abc123\",\"type\":\"welcome\"}";

            KuCoinTickerMessage message = objectMapper.readValue(welcome, KuCoinTickerMessage.class);

            assertTrue(message.isWelcome());
            assertFalse(message.isTickerMessage());
            assertFalse(message.isPong());
            assertFalse(message.isAck());
        }

        @Test
        @DisplayName("Subscription ack is correctly identified")
        void subscriptionAck_isIdentifiedCorrectly() throws Exception {
            String ack = "{\"id\":\"sub-1\",\"type\":\"ack\"}";

            KuCoinTickerMessage message = objectMapper.readValue(ack, KuCoinTickerMessage.class);

            assertTrue(message.isAck());
            assertFalse(message.isTickerMessage());
            assertFalse(message.isPong());
            assertFalse(message.isWelcome());
        }

        @Test
        @DisplayName("Pong message is correctly identified")
        void pongMessage_isIdentifiedCorrectly() throws Exception {
            String pong = "{\"id\":\"ping-0\",\"type\":\"pong\"}";

            KuCoinTickerMessage message = objectMapper.readValue(pong, KuCoinTickerMessage.class);

            assertTrue(message.isPong());
            assertFalse(message.isTickerMessage());
            assertFalse(message.isWelcome());
            assertFalse(message.isAck());
        }

        @Test
        @DisplayName("TickerData.isValid() returns false when any price field is null")
        void tickerData_invalidWhenPriceFieldNull() throws Exception {
            String json = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\","
                    + "\"data\":{\"bestBid\":\"21109.50\",\"time\":1673853746003}}";

            KuCoinTickerMessage message = objectMapper.readValue(json, KuCoinTickerMessage.class);

            assertFalse(message.isTickerMessage(),
                    "isTickerMessage() must be false when required fields are missing");
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    /**
     * Builds a minimal valid KuCoin ticker JSON string.
     */
    private String tickerJson(String bid, String bidSize, String ask, String askSize, long timeMs) {
        return "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                + "\"subject\":\"trade.ticker\",\"data\":{"
                + "\"bestBid\":\"" + bid + "\","
                + "\"bestBidSize\":\"" + bidSize + "\","
                + "\"bestAsk\":\"" + ask + "\","
                + "\"bestAskSize\":\"" + askSize + "\","
                + "\"time\":" + timeMs + "}}";
    }

    /**
     * Loads a test fixture file from the classpath.
     *
     * @param filename the fixture filename (e.g., "ticker-btcusdt-01.json")
     * @return the file contents as a string
     * @throws IOException if the fixture file cannot be read
     */
    private String loadFixture(String filename) throws IOException {
        Path fixturePath = Path.of("src/test/resources/fixtures/kucoin", filename);
        return Files.readString(fixturePath, StandardCharsets.UTF_8);
    }
}
