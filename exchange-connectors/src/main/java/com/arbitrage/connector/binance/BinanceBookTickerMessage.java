package com.arbitrage.connector.binance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Typed representation of a Binance Book Ticker WebSocket message.
 *
 * <p>Binance sends bookTicker updates as flat JSON with single-letter keys
 * to minimise bandwidth (critical at 1000+ messages/second). This POJO maps
 * those terse wire-format keys to descriptive Java field names using
 * {@link JsonProperty} — the compiler catches typos that {@code root.get("b")}
 * would silently turn into null at runtime.</p>
 *
 * <p><b>Wire format example:</b></p>
 * <pre>
 * {
 *   "u": 400900217,         // order book updateId
 *   "s": "BTCUSDT",         // symbol
 *   "b": "67250.50000000",  // best bid price
 *   "B": "1.23400000",      // best bid quantity
 *   "a": "67251.30000000",  // best ask price
 *   "A": "0.98700000"       // best ask quantity
 * }
 * </pre>
 *
 * <p><b>Why not use BigDecimal fields directly?</b> Jackson can deserialize
 * strings to BigDecimal, but we keep them as String here and convert in the
 * parser. This separates concerns: this class handles wire-format mapping
 * (JSON ↔ Java), while {@link BinanceMessageParser} handles domain conversion
 * (String → BigDecimal with precision validation). If a price field contains
 * "NaN" or garbage, the error is caught in the parser with proper context —
 * not buried in a Jackson deserialization exception with no field name.</p>
 *
 * <p><b>Why {@code @JsonIgnoreProperties(ignoreUnknown = true)}?</b> Binance
 * may add new fields to their API at any time. Without this annotation, an
 * unknown field would cause a deserialization failure — taking down the entire
 * feed. Defensive deserialization ignores unknown fields and only binds what
 * we need.</p>
 *
 * @see BinanceMessageParser for the conversion to {@link com.arbitrage.common.model.NormalisedTick}
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceBookTickerMessage {

    /**
     * Order book update ID — monotonically increasing sequence number.
     * Used by Binance to detect missed updates; not used in our pipeline
     * but captured for debugging and out-of-order detection.
     */
    @JsonProperty("u")
    private long updateId;

    /**
     * Binance symbol in their format (e.g., "BTCUSDT" — uppercase, no separator).
     * Our canonical format is "BTC-USDT"; the parser handles the translation.
     */
    @JsonProperty("s")
    private String symbol;

    /**
     * Best bid price as a string. Binance always sends prices as strings
     * to preserve decimal precision (JSON numbers lose trailing zeros).
     */
    @JsonProperty("b")
    private String bestBidPrice;

    /**
     * Quantity available at the best bid price.
     */
    @JsonProperty("B")
    private String bestBidQuantity;

    /**
     * Best ask price as a string.
     */
    @JsonProperty("a")
    private String bestAskPrice;

    /**
     * Quantity available at the best ask price.
     */
    @JsonProperty("A")
    private String bestAskQuantity;

    /**
     * Returns true if this message contains the minimum fields required
     * for a valid bookTicker update. Used by the parser to reject
     * malformed or unexpected message types before attempting conversion.
     *
     * @return true if all price and quantity fields are non-null
     */
    public boolean isValid() {
        return bestBidPrice != null
                && bestBidQuantity != null
                && bestAskPrice != null
                && bestAskQuantity != null;
    }
}
