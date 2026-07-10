package com.arbitrage.connector.nse;

import com.arbitrage.common.model.DataQuality;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link NseTickTransformer}.
 *
 * <p>Tests paise-to-INR conversion precision, token-to-pair reverse lookup,
 * and NormalisedTick assembly in isolation — no parser, no Spring context.
 * This isolation is why the transformer was separated from the parser.</p>
 */
class NseTickTransformerTest {

    private static final TradingPair RELIANCE_INR = TradingPair.builder()
            .baseCurrency("RELIANCE")
            .quoteCurrency("INR")
            .build();

    private static final TradingPair TCS_INR = TradingPair.builder()
            .baseCurrency("TCS")
            .quoteCurrency("INR")
            .build();

    private static final long RECEIVED_NANOS = 1_000_000_000L;

    private NseTickTransformer transformer;

    @BeforeEach
    void setUp() {
        Map<String, TradingPair> tokenMap = Map.of(
                "2885",  RELIANCE_INR,
                "11536", TCS_INR
        );
        transformer = new NseTickTransformer(tokenMap);
    }

    // ============================================================
    // Paise-to-INR conversion
    // ============================================================

    @ParameterizedTest(name = "{0} paise = ₹{1}")
    @CsvSource({
            "100,    1.00",
            "50,     0.50",
            "1,      0.01",
            "244900, 2449.00",
            "245100, 2451.00",
            "10000000, 100000.00",
            "1234567, 12345.67"
    })
    @DisplayName("paise-to-INR conversion produces correct BigDecimal")
    void paiseToInrConversion(long paise, String expectedRupees) {
        BigDecimal result = transformer.paiseToinr(paise);
        assertEquals(new BigDecimal(expectedRupees), result,
                () -> paise + " paise should be ₹" + expectedRupees);
    }

    @Test
    @DisplayName("paise conversion result has exactly 2 decimal places")
    void conversionScaleIsTwo() {
        BigDecimal result = transformer.paiseToinr(244900);
        assertEquals(2, result.scale(), "INR values must have scale=2");
    }

    // ============================================================
    // Token lookup
    // ============================================================

    @Test
    @DisplayName("token 2885 resolves to RELIANCE-INR")
    void tokenResolvesToReliance() {
        assertEquals(RELIANCE_INR, transformer.resolvePair("2885"));
    }

    @Test
    @DisplayName("token 11536 resolves to TCS-INR")
    void tokenResolvesToTcs() {
        assertEquals(TCS_INR, transformer.resolvePair("11536"));
    }

    @Test
    @DisplayName("unknown token returns null")
    void unknownTokenReturnsNull() {
        assertNull(transformer.resolvePair("9999"));
    }

    // ============================================================
    // transform() — mode-2 Quote
    // ============================================================

    @Test
    @DisplayName("transform() returns FULL_BOOK tick for valid quote message")
    void transformProducesFullBookTick() {
        NseQuoteMessage msg = buildQuoteMessage("2885", 244900, 100, 245100, 200);
        Optional<NormalisedTick> result = transformer.transform(msg, RECEIVED_NANOS);
        assertTrue(result.isPresent());
        assertEquals(DataQuality.FULL_BOOK, result.get().getDataQuality());
    }

    @Test
    @DisplayName("transform() sets exchange ID to NSE")
    void transformSetsExchangeIdNse() {
        NseQuoteMessage msg = buildQuoteMessage("2885", 244900, 100, 245100, 200);
        NormalisedTick tick = transformer.transform(msg, RECEIVED_NANOS).orElseThrow();
        assertEquals(ExchangeId.NSE, tick.getExchangeId());
    }

    @Test
    @DisplayName("transform() converts bid paise correctly")
    void transformConvertsBidPaise() {
        NseQuoteMessage msg = buildQuoteMessage("2885", 244900, 100, 245100, 200);
        NormalisedTick tick = transformer.transform(msg, RECEIVED_NANOS).orElseThrow();
        assertEquals(new BigDecimal("2449.00"), tick.getBestBidPrice());
    }

    @Test
    @DisplayName("transform() converts ask paise correctly")
    void transformConvertsAskPaise() {
        NseQuoteMessage msg = buildQuoteMessage("2885", 244900, 100, 245100, 200);
        NormalisedTick tick = transformer.transform(msg, RECEIVED_NANOS).orElseThrow();
        assertEquals(new BigDecimal("2451.00"), tick.getBestAskPrice());
    }

    @Test
    @DisplayName("transform() sets bid and ask quantities")
    void transformSetsQuantities() {
        NseQuoteMessage msg = buildQuoteMessage("2885", 244900, 150, 245100, 300);
        NormalisedTick tick = transformer.transform(msg, RECEIVED_NANOS).orElseThrow();
        assertEquals(new BigDecimal("150"), tick.getBestBidQuantity());
        assertEquals(new BigDecimal("300"), tick.getBestAskQuantity());
    }

    @Test
    @DisplayName("transform() returns empty for unconfigured token")
    void transformReturnsEmptyForUnknownToken() {
        NseQuoteMessage msg = buildQuoteMessage("9999", 244900, 100, 245100, 200);
        assertTrue(transformer.transform(msg, RECEIVED_NANOS).isEmpty());
    }

    // ============================================================
    // transformLtp() — mode-1 LTP
    // ============================================================

    @Test
    @DisplayName("transformLtp() returns LTP_ONLY tick")
    void transformLtpProducesLtpOnlyTick() {
        NseQuoteMessage msg = buildLtpMessage("2885", 245000);
        Optional<NormalisedTick> result = transformer.transformLtp(msg, RECEIVED_NANOS);
        assertTrue(result.isPresent());
        assertEquals(DataQuality.LTP_ONLY, result.get().getDataQuality());
    }

    @Test
    @DisplayName("transformLtp() sets both bid and ask to LTP value")
    void transformLtpSetsBidAskToLtp() {
        NseQuoteMessage msg = buildLtpMessage("2885", 245000);
        NormalisedTick tick = transformer.transformLtp(msg, RECEIVED_NANOS).orElseThrow();
        BigDecimal expectedLtp = new BigDecimal("2450.00");
        assertEquals(expectedLtp, tick.getBestBidPrice());
        assertEquals(expectedLtp, tick.getBestAskPrice());
    }

    @Test
    @DisplayName("transformLtp() returns empty for unconfigured token")
    void transformLtpReturnsEmptyForUnknownToken() {
        NseQuoteMessage msg = buildLtpMessage("9999", 245000);
        assertTrue(transformer.transformLtp(msg, RECEIVED_NANOS).isEmpty());
    }

    // ============================================================
    // Helpers
    // ============================================================

    private NseQuoteMessage buildQuoteMessage(String token,
                                               long bidPaise, long bidQty,
                                               long askPaise, long askQty) {
        NseQuoteMessage msg = new NseQuoteMessage();
        msg.setType("quote_data");
        msg.setToken(token);
        msg.setExchangeTimestamp(1688000000000L);
        msg.setLastTradedPrice(bidPaise);

        NseQuoteMessage.PriceLevel bid = new NseQuoteMessage.PriceLevel();
        bid.setPrice(bidPaise);
        bid.setQuantity(bidQty);
        msg.setBest5BuyData(List.of(bid));

        NseQuoteMessage.PriceLevel ask = new NseQuoteMessage.PriceLevel();
        ask.setPrice(askPaise);
        ask.setQuantity(askQty);
        msg.setBest5SellData(List.of(ask));

        return msg;
    }

    private NseQuoteMessage buildLtpMessage(String token, long ltpPaise) {
        NseQuoteMessage msg = new NseQuoteMessage();
        msg.setType("ltp_data");
        msg.setToken(token);
        msg.setExchangeTimestamp(1688000000000L);
        msg.setLastTradedPrice(ltpPaise);
        return msg;
    }
}
