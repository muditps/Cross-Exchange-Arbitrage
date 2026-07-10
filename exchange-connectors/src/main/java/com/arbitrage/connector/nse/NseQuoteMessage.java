package com.arbitrage.connector.nse;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * Wire-format POJO for Angel One SmartStream WebSocket messages.
 *
 * <p>Handles two message types relevant to market data:</p>
 * <ul>
 *   <li><b>quote_data</b> (mode=2) — Contains best-5 bid and ask levels. This is the primary
 *       market data message used for arbitrage spread calculation. Best bid =
 *       {@code best_5_buy_data[0].price}; best ask = {@code best_5_sell_data[0].price}.</li>
 *   <li><b>ltp_data</b> (mode=1) — Contains only {@code last_traded_price}. No live spread
 *       available. Produces a {@link com.arbitrage.common.model.DataQuality#LTP_ONLY} tick.</li>
 * </ul>
 *
 * <p>Other messages (subscription acks, error frames) have {@code type} values that do not
 * match either of the above — {@link #isQuoteData()} and {@link #isLtpData()} both return
 * false and the parser discards the message.</p>
 *
 * <p><b>Price encoding:</b> All {@code price} fields are integers in paise (1 paise = ₹0.01).
 * {@link NseTickTransformer} performs the paise-to-INR conversion before building the
 * {@link com.arbitrage.common.model.NormalisedTick}.</p>
 *
 * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} ensures forward compatibility —
 * new fields added by Angel One do not cause deserialization failures.</p>
 *
 * @see NseMessageParser for the component that deserializes these messages
 * @see NseTickTransformer for the component that converts them to NormalisedTick
 */
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NseQuoteMessage {

    private static final String TYPE_QUOTE_DATA = "quote_data";
    private static final String TYPE_LTP_DATA   = "ltp_data";

    /**
     * Message type identifier. Known values: {@code "quote_data"}, {@code "ltp_data"}.
     * Subscription acks and error frames have different type strings.
     */
    @JsonProperty("type")
    private String type;

    /**
     * Angel One instrument token (e.g., {@code "2885"} for RELIANCE).
     * Used by {@link NseTickTransformer} to reverse-look up the canonical {@code TradingPair}.
     */
    @JsonProperty("token")
    private String token;

    /**
     * NSE exchange server timestamp in epoch milliseconds.
     * More precise than crypto exchange timestamps; usable for clock skew measurement.
     */
    @JsonProperty("exchange_timestamp")
    private long exchangeTimestamp;

    /**
     * Last traded price in paise. Always present.
     * For {@code quote_data} messages, use {@code best_5_buy_data[0].price} and
     * {@code best_5_sell_data[0].price} instead — LTP is the most recent trade, which
     * may be seconds stale in thin markets.
     */
    @JsonProperty("last_traded_price")
    private long lastTradedPrice;

    /** Subscription mode that generated this message (1=LTP, 2=Quote, 3=SnapQuote). */
    @JsonProperty("subscription_mode")
    private int subscriptionMode;

    /**
     * Best-5 buy (bid) price levels, sorted descending by price.
     * Index 0 = best bid (highest price a buyer will pay).
     * Present only in mode-2 (Quote) and mode-3 (SnapQuote) messages.
     */
    @JsonProperty("best_5_buy_data")
    private List<PriceLevel> best5BuyData;

    /**
     * Best-5 sell (ask) price levels, sorted ascending by price.
     * Index 0 = best ask (lowest price a seller will accept).
     * Present only in mode-2 (Quote) and mode-3 (SnapQuote) messages.
     */
    @JsonProperty("best_5_sell_data")
    private List<PriceLevel> best5SellData;

    /**
     * Returns true if this is a mode-2 Quote message with usable bid and ask levels.
     * A message is only actionable for arbitrage detection if both sides of the book are present.
     */
    public boolean isQuoteData() {
        return TYPE_QUOTE_DATA.equals(type)
                && best5BuyData != null && !best5BuyData.isEmpty()
                && best5SellData != null && !best5SellData.isEmpty();
    }

    /**
     * Returns true if this is a mode-1 LTP message (last traded price only, no spread).
     * These messages are valid but insufficient for arbitrage — they produce
     * {@link com.arbitrage.common.model.DataQuality#LTP_ONLY} ticks.
     */
    public boolean isLtpData() {
        return TYPE_LTP_DATA.equals(type);
    }

    /**
     * A single price level in the best-5 buy or sell array.
     *
     * <p><b>Price unit: paise.</b> {@code price = 244900} means ₹2449.00.
     * {@link NseTickTransformer} converts to INR using {@code BigDecimal.divide(100)}.</p>
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PriceLevel {

        /** Price in paise (1 paisa = ₹0.01). */
        @JsonProperty("price")
        private long price;

        /** Available quantity at this price level. */
        @JsonProperty("quantity")
        private long quantity;

        /** Number of orders aggregated at this price level. */
        @JsonProperty("no_of_orders")
        private int noOfOrders;
    }
}
