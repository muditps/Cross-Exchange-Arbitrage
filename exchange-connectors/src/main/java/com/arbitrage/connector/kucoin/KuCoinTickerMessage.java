package com.arbitrage.connector.kucoin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Typed representation of a KuCoin WebSocket message.
 *
 * <p>KuCoin sends several different message types over the same WebSocket connection.
 * All messages share a top-level {@code type} field that discriminates between them:</p>
 *
 * <ul>
 *   <li>{@code "welcome"} — Sent once on connection. Must be received before subscribing.</li>
 *   <li>{@code "ack"} — Subscription confirmation. One per subscribe request.</li>
 *   <li>{@code "pong"} — Server response to our {@code {"type":"ping"}} heartbeat.</li>
 *   <li>{@code "message"} — Live market data. Contains the {@code data} object with prices.</li>
 * </ul>
 *
 * <p><b>Wire format — ticker message:</b></p>
 * <pre>
 * {
 *   "type": "message",
 *   "topic": "/market/ticker:BTC-USDT",
 *   "subject": "trade.ticker",
 *   "data": {
 *     "sequence": "1545896668986",
 *     "price": "21110.00",
 *     "size": "0.001",
 *     "bestAsk": "21109.60",
 *     "bestAskSize": "0.30000000",
 *     "bestBid": "21109.50",
 *     "bestBidSize": "0.50000000",
 *     "time": 1673853746003
 *   }
 * }
 * </pre>
 *
 * <p><b>Wire format — subscription ack:</b></p>
 * <pre>
 * {"id": "sub-btcusdt", "type": "ack"}
 * </pre>
 *
 * <p><b>Wire format — welcome:</b></p>
 * <pre>
 * {"id": "abc123", "type": "welcome"}
 * </pre>
 *
 * <p><b>Wire format — pong:</b></p>
 * <pre>
 * {"id": "ping-0", "type": "pong"}
 * </pre>
 *
 * <p><b>Why a single class handles all types:</b> Using one POJO with
 * {@code @JsonIgnoreProperties(ignoreUnknown = true)} is simpler than a type hierarchy.
 * Fields absent in a given message type are null. The helper methods
 * ({@link #isTickerMessage()}, {@link #isPong()}, etc.) provide type-safe discrimination.
 * This approach is consistent with how we handle Bybit messages.</p>
 *
 * <p><b>KuCoin symbol quirk (a welcome surprise):</b> KuCoin uses {@code BTC-USDT}
 * (uppercase, hyphen-separated) in its topic names — which is identical to our canonical
 * symbol format. This means no symbol conversion is needed for KuCoin. The canonical
 * symbol can be passed directly to the subscription topic, unlike Binance ({@code btcusdt})
 * or Bybit ({@code BTCUSDT}) which require transformation.</p>
 *
 * @see KuCoinMessageParser for conversion to {@link com.arbitrage.common.model.NormalisedTick}
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KuCoinTickerMessage {

    /**
     * Message discriminator. One of: {@code "welcome"}, {@code "ack"}, {@code "pong"},
     * {@code "message"}. This is the first field to check when routing incoming messages.
     */
    private String type;

    /**
     * Message topic. Present only in ticker messages (type="message").
     * Format: {@code "/market/ticker:BTC-USDT"}. Used to identify which
     * trading pair this message belongs to.
     */
    private String topic;

    /**
     * Sub-category of the message within the topic.
     * For ticker messages, always {@code "trade.ticker"}.
     */
    private String subject;

    /**
     * Message identifier. Present in control messages (welcome, ack, pong).
     * Absent (null) in ticker messages. Used to correlate pongs with pings.
     */
    private String id;

    /**
     * Live market data. Present only when {@code type="message"} and the topic
     * is a ticker topic. Null for all control messages.
     */
    private TickerData data;

    /**
     * Returns true if this is a live ticker update with all required price fields.
     *
     * @return true if this is a ticker data message
     */
    public boolean isTickerMessage() {
        return "message".equals(type) && data != null && data.isValid();
    }

    /**
     * Returns true if this is a pong response to one of our heartbeat pings.
     *
     * @return true if this is a pong control message
     */
    public boolean isPong() {
        return "pong".equals(type);
    }

    /**
     * Returns true if this is the welcome message sent by KuCoin on connection.
     *
     * @return true if this is a welcome message
     */
    public boolean isWelcome() {
        return "welcome".equals(type);
    }

    /**
     * Returns true if this is an acknowledgement for our subscription request.
     *
     * @return true if this is a subscription ack
     */
    public boolean isAck() {
        return "ack".equals(type);
    }

    /**
     * Nested ticker data from KuCoin's {@code /market/ticker} stream.
     *
     * <p>Contains the top-of-book bid and ask prices with quantities, a last-traded
     * price, and a millisecond timestamp from the exchange server. The {@code time}
     * field is used as the exchange timestamp in {@link com.arbitrage.common.model.NormalisedTick},
     * enabling accurate clock skew measurement between KuCoin and our system.</p>
     *
     * <p>{@code @JsonIgnoreProperties(ignoreUnknown = true)} is essential because KuCoin's
     * ticker message includes many fields we don't need ({@code sequence}, {@code price},
     * {@code size}). New fields added by KuCoin in future API updates will be silently
     * ignored rather than causing deserialization failures.</p>
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TickerData {

        /** Best bid price at the top of the order book. */
        private String bestBid;

        /** Quantity available at the best bid price. */
        private String bestBidSize;

        /** Best ask price at the top of the order book. */
        private String bestAsk;

        /** Quantity available at the best ask price. */
        private String bestAskSize;

        /**
         * Exchange server timestamp in epoch milliseconds.
         * Used for the exchange timestamp in NormalisedTick (same as Bybit's {@code ts}).
         */
        private long time;

        /**
         * Returns true if all required price and quantity fields are present.
         *
         * @return true if all four price/quantity fields are non-null and non-empty
         */
        public boolean isValid() {
            return bestBid != null && !bestBid.isEmpty()
                    && bestBidSize != null && !bestBidSize.isEmpty()
                    && bestAsk != null && !bestAsk.isEmpty()
                    && bestAskSize != null && !bestAskSize.isEmpty();
        }
    }
}
