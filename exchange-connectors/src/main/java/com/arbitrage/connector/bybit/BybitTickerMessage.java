package com.arbitrage.connector.bybit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Typed representation of a Bybit V5 Spot orderbook.1 WebSocket message.
 *
 * <p><b>Why orderbook.1 instead of tickers?</b> The {@code tickers.<symbol>} stream
 * sends only OHLCV statistics (lastPrice, volume24h, etc.) in snapshot messages —
 * bid1Price and ask1Price are absent. The {@code orderbook.1.<symbol>} stream always
 * includes the best bid and ask on every update (both snapshot and delta), which is
 * exactly what the arbitrage detector needs for cross-exchange spread calculation.</p>
 *
 * <p><b>Wire format example (orderbook.1 snapshot):</b></p>
 * <pre>
 * {
 *   "topic": "orderbook.1.BTCUSDT",
 *   "type": "snapshot",
 *   "ts": 1672304485581,
 *   "data": {
 *     "s": "BTCUSDT",
 *     "b": [["16493.50", "0.006"]],
 *     "a": [["16494.00", "1.200"]],
 *     "u": 18521288,
 *     "seq": 7961638724
 *   },
 *   "cts": 1672304485581
 * }
 * </pre>
 *
 * <p><b>Wire format example (orderbook.1 delta):</b></p>
 * <pre>
 * {
 *   "topic": "orderbook.1.BTCUSDT",
 *   "type": "delta",
 *   "ts": 1672304485700,
 *   "data": {
 *     "s": "BTCUSDT",
 *     "b": [["16494.00", "0.012"]],
 *     "a": [["16494.50", "0.800"]],
 *     "u": 18521289,
 *     "seq": 7961638725
 *   },
 *   "cts": 1672304485700
 * }
 * </pre>
 *
 * <p><b>Wire format example (subscription ack):</b></p>
 * <pre>
 * {"success": true, "ret_msg": "subscribe", "conn_id": "abc123", "req_id": "1", "op": "subscribe"}
 * </pre>
 *
 * <p><b>Wire format example (pong):</b></p>
 * <pre>
 * {"success": true, "ret_msg": "pong", "conn_id": "abc123", "req_id": "heartbeat", "op": "pong"}
 * </pre>
 *
 * @see BybitMessageParser for the conversion to {@link com.arbitrage.common.model.NormalisedTick}
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BybitTickerMessage {

    /**
     * Topic name indicating the stream (e.g., "orderbook.1.BTCUSDT").
     * Present in data messages, absent in control messages.
     */
    private String topic;

    /**
     * Bybit server timestamp in epoch milliseconds.
     * Used as the exchange timestamp in NormalisedTick for clock skew measurement.
     */
    private long ts;

    /**
     * Message type: "snapshot" (full state) or "delta" (incremental update).
     * Both types include bid/ask entries when data is available.
     */
    private String type;

    /**
     * Nested orderbook data containing best bid and ask.
     * Null for control messages (subscription acks, pongs).
     */
    private OrderbookData data;

    /**
     * Operation type for control messages: "subscribe" (ack) or "pong" (heartbeat response).
     * Null for data messages.
     */
    private String op;

    /**
     * Success flag for control messages. Null for data messages.
     */
    private Boolean success;

    /**
     * Returns true if this message contains valid orderbook data with both bid and ask.
     *
     * @return true if this is a data message with at least one bid and one ask entry
     */
    public boolean isTickerMessage() {
        return data != null && data.isValid();
    }

    /**
     * Returns true if this message is a pong response to a client heartbeat ping.
     *
     * @return true if this is a pong control message
     */
    public boolean isPong() {
        return "pong".equals(op);
    }

    /**
     * Returns true if this message is a subscription acknowledgment.
     *
     * @return true if this is a subscription ack control message
     */
    public boolean isSubscriptionAck() {
        return "subscribe".equals(op);
    }

    /**
     * Nested orderbook data from Bybit's V5 depth-1 orderbook stream.
     *
     * <p>Contains the best bid and ask as arrays of [price, size] pairs. Bybit uses
     * compact single-character field names (s, b, a) in the orderbook stream unlike
     * the verbose names in the tickers stream.</p>
     *
     * <p>For snapshot messages: both {@code bids} and {@code asks} always contain
     * one entry each. For delta messages: both sides are typically included, but
     * an empty array means that side did not change. Messages where either side is
     * empty are skipped (see {@link #isValid()}) — the next message will have both.</p>
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderbookData {

        /** Symbol (e.g., "BTCUSDT"). */
        @JsonProperty("s")
        private String symbol;

        /** Bids: list of [price, size] string pairs, best bid first. */
        @JsonProperty("b")
        private List<List<String>> bids;

        /** Asks: list of [price, size] string pairs, best ask first. */
        @JsonProperty("a")
        private List<List<String>> asks;

        /** Update ID for sequencing. */
        @JsonProperty("u")
        private long updateId;

        /** Sequence number for gap detection. */
        @JsonProperty("seq")
        private long seq;

        /**
         * Returns true if this message contains usable orderbook data on at least one side.
         *
         * <p>Bybit {@code orderbook.1} delta messages frequently only include the changed side
         * (e.g. only bids when the ask is unchanged). Accepting single-side messages here
         * allows {@link com.arbitrage.connector.bybit.BybitMessageParser} to merge them with
         * its stored last-known state and emit a complete tick on every price change.
         *
         * <p>Requiring both sides (the original behaviour) caused 5–10 second gaps because we
         * only processed messages where bid and ask happened to change simultaneously.
         *
         * @return true if either bid[0] or ask[0] has at least two elements (price, size)
         */
        public boolean isValid() {
            return hasBid() || hasAsk();
        }

        /**
         * Returns true if this message contains a valid best-bid entry.
         *
         * @return true if bids[0] has at least two elements (price, size)
         */
        public boolean hasBid() {
            return bids != null && !bids.isEmpty() && bids.get(0).size() >= 2;
        }

        /**
         * Returns true if this message contains a valid best-ask entry.
         *
         * @return true if asks[0] has at least two elements (price, size)
         */
        public boolean hasAsk() {
            return asks != null && !asks.isEmpty() && asks.get(0).size() >= 2;
        }

        /** @return best bid price string, or null if bids is empty */
        public String bestBidPrice() {
            return (bids != null && !bids.isEmpty()) ? bids.get(0).get(0) : null;
        }

        /** @return best bid size string, or null if bids is empty */
        public String bestBidSize() {
            return (bids != null && !bids.isEmpty()) ? bids.get(0).get(1) : null;
        }

        /** @return best ask price string, or null if asks is empty */
        public String bestAskPrice() {
            return (asks != null && !asks.isEmpty()) ? asks.get(0).get(0) : null;
        }

        /** @return best ask size string, or null if asks is empty */
        public String bestAskSize() {
            return (asks != null && !asks.isEmpty()) ? asks.get(0).get(1) : null;
        }
    }
}
