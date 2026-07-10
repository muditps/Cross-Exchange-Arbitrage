package com.arbitrage.connector.nse;

import com.arbitrage.common.model.DataQuality;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link NseMessageParser}.
 *
 * <p>Uses recorded Angel One SmartAPI fixture files from
 * {@code src/test/resources/fixtures/nse/}. The transformer is constructed with
 * a test token map (no Spring context needed) so tests are fast and isolated.</p>
 *
 * <p><b>Critical correctness checks:</b>
 * <ul>
 *   <li>Paise-to-INR conversion: 244900 paise → ₹2449.00, 245100 paise → ₹2451.00</li>
 *   <li>Token resolution: "2885" → RELIANCE-INR TradingPair</li>
 *   <li>DataQuality.FULL_BOOK for mode-2 Quote messages</li>
 *   <li>DataQuality.LTP_ONLY for mode-1 LTP messages</li>
 * </ul>
 * </p>
 */
class NseMessageParserTest {

    private static final TradingPair RELIANCE_INR = TradingPair.builder()
            .baseCurrency("RELIANCE")
            .quoteCurrency("INR")
            .build();

    private static final long RECEIVED_NANOS = 1_000_000_000L;

    private NseMessageParser parser;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        Map<String, TradingPair> tokenMap = Map.of("2885", RELIANCE_INR);
        NseTickTransformer transformer = new NseTickTransformer(tokenMap);
        parser = new NseMessageParser(objectMapper, metrics, transformer);
    }

    // ============================================================
    // Happy Path — mode-2 Quote message
    // ============================================================

    @Nested
    @DisplayName("mode-2 Quote message (quote_data)")
    class QuoteDataTests {

        @Test
        @DisplayName("parses fixture and returns FULL_BOOK tick")
        void parsesQuoteDataFixture() throws IOException {
            String json = loadFixture("quote_data_reliance.json");
            Optional<NormalisedTick> result = parser.parse(json, null, RECEIVED_NANOS);
            assertTrue(result.isPresent(), "Expected a tick for a valid quote_data message");
        }

        @Test
        @DisplayName("exchange ID is NSE")
        void exchangeIdIsNse() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(ExchangeId.NSE, tick.getExchangeId());
        }

        @Test
        @DisplayName("trading pair is RELIANCE-INR via token lookup")
        void tradingPairResolvedFromToken() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(RELIANCE_INR, tick.getTradingPair());
        }

        @Test
        @DisplayName("best bid price is 244900 paise → ₹2449.00")
        void bestBidPriceConvertedFromPaise() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(new BigDecimal("2449.00"), tick.getBestBidPrice());
        }

        @Test
        @DisplayName("best ask price is 245100 paise → ₹2451.00")
        void bestAskPriceConvertedFromPaise() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(new BigDecimal("2451.00"), tick.getBestAskPrice());
        }

        @Test
        @DisplayName("best bid quantity from best_5_buy_data[0]")
        void bestBidQuantity() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(new BigDecimal("100"), tick.getBestBidQuantity());
        }

        @Test
        @DisplayName("best ask quantity from best_5_sell_data[0]")
        void bestAskQuantity() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(new BigDecimal("200"), tick.getBestAskQuantity());
        }

        @Test
        @DisplayName("dataQuality is FULL_BOOK for mode-2 Quote")
        void dataQualityIsFullBook() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(DataQuality.FULL_BOOK, tick.getDataQuality());
        }

        @Test
        @DisplayName("receivedTimestamp is the T0 value passed in")
        void receivedTimestampPreserved() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertEquals(RECEIVED_NANOS, tick.getReceivedTimestamp());
        }

        @Test
        @DisplayName("processedTimestamp is after receivedTimestamp")
        void processedTimestampAfterReceived() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertTrue(tick.getProcessedTimestamp() >= tick.getReceivedTimestamp(),
                    "processedTimestamp must not precede receivedTimestamp");
        }

        @Test
        @DisplayName("exchangeTimestamp derived from exchange_timestamp epoch millis")
        void exchangeTimestampFromEpochMillis() throws IOException {
            NormalisedTick tick = parseFixture("quote_data_reliance.json");
            assertNotNull(tick.getExchangeTimestamp());
            assertEquals(1688000000000L, tick.getExchangeTimestamp().toEpochMilli());
        }
    }

    // ============================================================
    // LTP-only message (mode-1)
    // ============================================================

    @Nested
    @DisplayName("mode-1 LTP message (ltp_data)")
    class LtpDataTests {

        @Test
        @DisplayName("parses ltp_data fixture and returns LTP_ONLY tick")
        void parsesLtpFixture() throws IOException {
            String json = loadFixture("ltp_data_reliance.json");
            Optional<NormalisedTick> result = parser.parse(json, null, RECEIVED_NANOS);
            assertTrue(result.isPresent(), "Expected a tick for a valid ltp_data message");
        }

        @Test
        @DisplayName("dataQuality is LTP_ONLY for mode-1 LTP message")
        void dataQualityIsLtpOnly() throws IOException {
            String json = loadFixture("ltp_data_reliance.json");
            NormalisedTick tick = parser.parse(json, null, RECEIVED_NANOS).orElseThrow();
            assertEquals(DataQuality.LTP_ONLY, tick.getDataQuality());
        }

        @Test
        @DisplayName("bid and ask are both set to LTP value (2450.00)")
        void bidAskBothSetToLtp() throws IOException {
            String json = loadFixture("ltp_data_reliance.json");
            NormalisedTick tick = parser.parse(json, null, RECEIVED_NANOS).orElseThrow();
            BigDecimal expectedLtp = new BigDecimal("2450.00");
            assertEquals(expectedLtp, tick.getBestBidPrice());
            assertEquals(expectedLtp, tick.getBestAskPrice());
        }
    }

    // ============================================================
    // Skip cases
    // ============================================================

    @Nested
    @DisplayName("Non-market-data messages are discarded")
    class SkipCaseTests {

        @Test
        @DisplayName("subscription ack returns empty")
        void subscriptionAckIsSkipped() throws IOException {
            String json = loadFixture("subscription_ack.json");
            Optional<NormalisedTick> result = parser.parse(json, null, RECEIVED_NANOS);
            assertTrue(result.isEmpty(), "Subscription ack should not produce a tick");
        }

        @Test
        @DisplayName("malformed JSON returns empty without throwing")
        void malformedJsonReturnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("{not valid json", null, RECEIVED_NANOS);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("empty string returns empty without throwing")
        void emptyStringReturnsEmpty() {
            Optional<NormalisedTick> result = parser.parse("", null, RECEIVED_NANOS);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("unknown token returns empty")
        void unknownTokenReturnsEmpty() {
            String json = """
                    {"type":"quote_data","token":"9999",
                     "exchange_timestamp":1688000000000,
                     "best_5_buy_data":[{"price":244900,"quantity":100,"no_of_orders":1}],
                     "best_5_sell_data":[{"price":245100,"quantity":200,"no_of_orders":1}]}
                    """;
            Optional<NormalisedTick> result = parser.parse(json, null, RECEIVED_NANOS);
            assertTrue(result.isEmpty(), "Tick for unconfigured token 9999 should be discarded");
        }

        @Test
        @DisplayName("quote_data with empty buy array returns empty")
        void emptyBuyArrayReturnsEmpty() {
            String json = """
                    {"type":"quote_data","token":"2885",
                     "exchange_timestamp":1688000000000,
                     "best_5_buy_data":[],
                     "best_5_sell_data":[{"price":245100,"quantity":200,"no_of_orders":1}]}
                    """;
            Optional<NormalisedTick> result = parser.parse(json, null, RECEIVED_NANOS);
            assertTrue(result.isEmpty(), "Quote message with empty buy side should not produce a tick");
        }
    }

    // ============================================================
    // Paise conversion edge cases
    // ============================================================

    @Nested
    @DisplayName("Paise-to-INR conversion precision")
    class PaiseConversionTests {

        @Test
        @DisplayName("round number: 100000 paise = ₹1000.00")
        void roundNumberConversion() {
            String json = buildQuoteJson("2885", 100000, 100100);
            NormalisedTick tick = parser.parse(json, null, RECEIVED_NANOS).orElseThrow();
            assertEquals(new BigDecimal("1000.00"), tick.getBestBidPrice());
            assertEquals(new BigDecimal("1001.00"), tick.getBestAskPrice());
        }

        @Test
        @DisplayName("single-digit rupee: 50 paise = ₹0.50")
        void smallPriceConversion() {
            String json = buildQuoteJson("2885", 50, 75);
            NormalisedTick tick = parser.parse(json, null, RECEIVED_NANOS).orElseThrow();
            assertEquals(new BigDecimal("0.50"), tick.getBestBidPrice());
            assertEquals(new BigDecimal("0.75"), tick.getBestAskPrice());
        }

        @Test
        @DisplayName("large price: 10000000 paise = ₹100000.00")
        void largePriceConversion() {
            String json = buildQuoteJson("2885", 10_000_000, 10_001_000);
            NormalisedTick tick = parser.parse(json, null, RECEIVED_NANOS).orElseThrow();
            assertEquals(new BigDecimal("100000.00"), tick.getBestBidPrice());
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    private NormalisedTick parseFixture(String filename) throws IOException {
        return parser.parse(loadFixture(filename), null, RECEIVED_NANOS)
                .orElseThrow(() -> new AssertionError("Expected tick from fixture: " + filename));
    }

    private String loadFixture(String filename) throws IOException {
        Path path = Path.of("src/test/resources/fixtures/nse/" + filename);
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    private String buildQuoteJson(String token, long bidPaise, long askPaise) {
        return String.format("""
                {"type":"quote_data","token":"%s",
                 "exchange_timestamp":1688000000000,
                 "best_5_buy_data":[{"price":%d,"quantity":100,"no_of_orders":1}],
                 "best_5_sell_data":[{"price":%d,"quantity":200,"no_of_orders":1}]}
                """, token, bidPaise, askPaise);
    }
}
