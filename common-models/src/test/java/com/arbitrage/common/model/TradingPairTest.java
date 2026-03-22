package com.arbitrage.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TradingPair} — the canonical, exchange-agnostic asset pair representation.
 *
 * <p>These tests verify:
 * <ul>
 *   <li>Canonical symbol format (BASE-QUOTE, uppercase, hyphen-separated)</li>
 *   <li>Factory method parsing and validation</li>
 *   <li>Value equality (two TradingPairs with the same currencies are {@code .equals()})</li>
 *   <li>Asset-class agnosticism (works for crypto AND equities)</li>
 * </ul>
 */
class TradingPairTest {

    @Nested
    @DisplayName("canonicalSymbol()")
    class CanonicalSymbolTests {

        @Test
        @DisplayName("formats as BASE-QUOTE with hyphen separator")
        void formatsWithHyphenSeparator() {
            TradingPair pair = TradingPair.builder()
                    .baseCurrency("BTC")
                    .quoteCurrency("USDT")
                    .build();

            assertEquals("BTC-USDT", pair.canonicalSymbol());
        }

        @Test
        @DisplayName("works for Indian equity pairs (RELIANCE-INR)")
        void worksForIndianEquityPairs() {
            TradingPair pair = TradingPair.builder()
                    .baseCurrency("RELIANCE")
                    .quoteCurrency("INR")
                    .build();

            assertEquals("RELIANCE-INR", pair.canonicalSymbol());
        }

        @Test
        @DisplayName("toString() delegates to canonicalSymbol()")
        void toStringDelegatesToCanonicalSymbol() {
            TradingPair pair = TradingPair.builder()
                    .baseCurrency("ETH")
                    .quoteCurrency("BTC")
                    .build();

            assertEquals(pair.canonicalSymbol(), pair.toString());
        }
    }

    @Nested
    @DisplayName("fromSymbol()")
    class FromSymbolTests {

        @ParameterizedTest(name = "\"{0}\" -> base={1}, quote={2}")
        @CsvSource({
                "BTC-USDT,   BTC,   USDT",
                "ETH-BTC,    ETH,   BTC",
                "RELIANCE-INR, RELIANCE, INR",
                "TCS-INR,    TCS,   INR",
                "DOGE-USDT,  DOGE,  USDT"
        })
        @DisplayName("parses valid symbols correctly")
        void parsesValidSymbols(String symbol, String expectedBase, String expectedQuote) {
            TradingPair pair = TradingPair.fromSymbol(symbol);

            assertEquals(expectedBase, pair.getBaseCurrency());
            assertEquals(expectedQuote, pair.getQuoteCurrency());
        }

        @Test
        @DisplayName("converts lowercase input to uppercase")
        void convertsToUppercase() {
            TradingPair pair = TradingPair.fromSymbol("btc-usdt");

            assertEquals("BTC", pair.getBaseCurrency());
            assertEquals("USDT", pair.getQuoteCurrency());
        }

        @Test
        @DisplayName("converts mixed case input to uppercase")
        void convertsMixedCaseToUppercase() {
            TradingPair pair = TradingPair.fromSymbol("Btc-Usdt");

            assertEquals("BTC", pair.getBaseCurrency());
            assertEquals("USDT", pair.getQuoteCurrency());
        }

        @Test
        @DisplayName("throws NullPointerException for null input")
        void throwsOnNullInput() {
            assertThrows(NullPointerException.class, () -> TradingPair.fromSymbol(null));
        }

        @ParameterizedTest(name = "rejects invalid symbol: \"{0}\"")
        @ValueSource(strings = {
                "",           // empty string
                "BTCUSDT",    // no separator (Binance format)
                "BTC_USDT",   // wrong separator
                "BTC/USDT",   // wrong separator
                "-USDT",      // missing base
                "BTC-",       // missing quote
                "-",          // only separator
                "A-B-C"       // too many parts
        })
        @DisplayName("throws IllegalArgumentException for invalid formats")
        void throwsOnInvalidFormats(String invalidSymbol) {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> TradingPair.fromSymbol(invalidSymbol)
            );

            assertTrue(exception.getMessage().contains("Invalid trading pair symbol"));
        }
    }

    @Nested
    @DisplayName("Value equality")
    class EqualityTests {

        @Test
        @DisplayName("two pairs with same currencies are equal")
        void sameCurrenciesAreEqual() {
            TradingPair pair1 = TradingPair.builder()
                    .baseCurrency("BTC")
                    .quoteCurrency("USDT")
                    .build();
            TradingPair pair2 = TradingPair.builder()
                    .baseCurrency("BTC")
                    .quoteCurrency("USDT")
                    .build();

            assertEquals(pair1, pair2);
            assertEquals(pair1.hashCode(), pair2.hashCode());
        }

        @Test
        @DisplayName("pairs with different base currencies are not equal")
        void differentBaseCurrenciesAreNotEqual() {
            TradingPair btcUsdt = TradingPair.fromSymbol("BTC-USDT");
            TradingPair ethUsdt = TradingPair.fromSymbol("ETH-USDT");

            assertNotEquals(btcUsdt, ethUsdt);
        }

        @Test
        @DisplayName("pairs with different quote currencies are not equal")
        void differentQuoteCurrenciesAreNotEqual() {
            TradingPair btcUsdt = TradingPair.fromSymbol("BTC-USDT");
            TradingPair btcEth = TradingPair.fromSymbol("BTC-ETH");

            assertNotEquals(btcUsdt, btcEth);
        }

        @Test
        @DisplayName("fromSymbol result equals builder result")
        void fromSymbolEqualsBuilder() {
            TradingPair fromSymbol = TradingPair.fromSymbol("BTC-USDT");
            TradingPair fromBuilder = TradingPair.builder()
                    .baseCurrency("BTC")
                    .quoteCurrency("USDT")
                    .build();

            assertEquals(fromSymbol, fromBuilder);
        }

        @Test
        @DisplayName("can be used as HashMap key")
        void worksAsHashMapKey() {
            TradingPair key1 = TradingPair.fromSymbol("BTC-USDT");
            TradingPair key2 = TradingPair.fromSymbol("BTC-USDT");

            java.util.Map<TradingPair, String> map = new java.util.HashMap<>();
            map.put(key1, "value");

            assertEquals("value", map.get(key2),
                    "TradingPair must work as HashMap key — equal pairs must retrieve the same value");
        }
    }
}
