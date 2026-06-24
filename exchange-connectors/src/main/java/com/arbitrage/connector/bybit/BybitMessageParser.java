package com.arbitrage.connector.bybit;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.TradingPairsProperties;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parses raw Bybit V5 {@code orderbook.1} WebSocket messages into {@link NormalisedTick} objects.
 *
 * <p><b>Responsibility boundary:</b> This class handles <em>data transformation</em> only —
 * JSON string → typed POJO → domain object. It does NOT handle connection lifecycle,
 * T0 timestamp capture, heartbeat pings, feed status, or sink emission. Those concerns
 * belong to {@link BybitConnector}.</p>
 *
 * <p><b>Why orderbook.1 instead of tickers?</b> The Bybit V5 {@code tickers} stream sends
 * only OHLCV statistics in snapshot messages — bid1Price and ask1Price are absent. The
 * {@code orderbook.1} stream always includes the best bid and ask on every update, which
 * is exactly what arbitrage detection requires.</p>
 *
 * <p><b>Key difference from BinanceMessageParser:</b> Bybit provides a server timestamp
 * ({@code ts}) in every message. This parser uses {@code Instant.ofEpochMilli(ts)}
 * for the exchange timestamp instead of {@code Instant.now()}. This is strictly better
 * because it reflects when Bybit generated the data, not when we received it — enabling
 * accurate clock skew measurement between the exchange and our system.</p>
 *
 * <p><b>Stateful delta merging:</b> Bybit {@code orderbook.1} delta messages frequently
 * only include the changed side (bid or ask) — the other side is omitted if unchanged.
 * This class maintains a per-symbol last-known bid/ask state in {@code lastPrices} so
 * that every delta produces a complete tick rather than being discarded as "incomplete".
 * Without this, only messages where bid and ask change simultaneously are processed,
 * causing 5–10 second gaps in the live monitor.</p>
 *
 * <p><b>Thread safety:</b> {@code lastPrices} uses a {@link ConcurrentHashMap} for
 * safe concurrent access. In practice, all Bybit messages for one connection are
 * processed serially on a single Reactor event-loop thread, so the array entries are
 * never written concurrently. The ConcurrentHashMap guards against hypothetical future
 * parallelism or Spring AOP proxies.</p>
 *
 * <p><b>Latency note:</b> Uses Jackson's full object mapping ({@code readValue}) rather than
 * streaming API ({@code JsonParser}). Bybit's nested format adds ~2-5µs over Binance's flat
 * format due to the inner object traversal. If p99 parsing latency exceeds 50µs under load
 * (measured in Phase 6), switch to the streaming API. Profile before optimising.</p>
 *
 * @see BybitTickerMessage for the wire-format POJO
 * @see BybitConnector for the connection lifecycle that calls this parser
 */
@Component
@Slf4j
public class BybitMessageParser {

    private final ObjectMapper objectMapper;
    private final ConnectorMetrics metrics;

    /**
     * Lookup map from Bybit symbol (e.g. "BTCUSDT") to canonical TradingPair.
     * Empty in single-pair / test mode — falls back to the tradingPair parameter.
     */
    private final Map<String, TradingPair> symbolLookup;

    /**
     * Last-known best bid and ask per Bybit symbol, indexed as:
     * [0]=bestBidPrice, [1]=bestBidQty, [2]=bestAskPrice, [3]=bestAskQty.
     *
     * <p>Populated on first snapshot message (always has both sides). Subsequent
     * delta messages update only the side(s) included, then emit using merged state.</p>
     */
    private final ConcurrentHashMap<String, BigDecimal[]> lastPrices = new ConcurrentHashMap<>();

    /**
     * Single-pair constructor for tests — no symbol lookup, falls back to tradingPair param.
     *
     * @param objectMapper pre-configured Jackson ObjectMapper (singleton)
     * @param metrics      connector metrics
     */
    public BybitMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics) {
        this(objectMapper, metrics, null);
    }

    /**
     * Multi-pair constructor used by Spring. Builds a symbol→pair lookup from all configured pairs.
     *
     * @param objectMapper          pre-configured Jackson ObjectMapper (singleton)
     * @param metrics               connector metrics for recording parse outcomes and latency
     * @param tradingPairsProperties configured trading pairs; null produces empty lookup (test mode)
     */
    @Autowired
    public BybitMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics,
                              TradingPairsProperties tradingPairsProperties) {
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.symbolLookup = buildSymbolLookup(tradingPairsProperties);
    }

    private static Map<String, TradingPair> buildSymbolLookup(TradingPairsProperties props) {
        if (props == null || props.getPairs() == null) {
            return Map.of();
        }
        Map<String, TradingPair> lookup = new HashMap<>();
        for (TradingPair pair : props.asTradingPairs()) {
            // Bybit symbol format: uppercase, no separator (BTC-USDT → BTCUSDT)
            lookup.put((pair.getBaseCurrency() + pair.getQuoteCurrency()).toUpperCase(), pair);
        }
        return lookup;
    }

    /**
     * Parses a raw Bybit WebSocket message into a {@link NormalisedTick}.
     *
     * <p>Returns {@link Optional#empty()} for messages that are not ticker updates
     * (subscription acks, pong responses, unknown formats). This is intentional —
     * control messages are normal protocol traffic, not errors.</p>
     *
     * <p><b>Error handling strategy:</b> Malformed JSON and invalid prices are logged
     * and return {@code Optional.empty()}. They do NOT throw exceptions. In a hot
     * reactive stream, an uncaught exception would terminate all subscribers. One bad
     * message should not kill the feed.</p>
     *
     * @param rawJson       the raw JSON string from the WebSocket
     * @param tradingPair   the trading pair this message belongs to
     * @param receivedNanos T0 timestamp (System.nanoTime captured by the connector
     *                      as the first operation on message arrival)
     * @return the parsed NormalisedTick, or empty if the message is not a ticker update
     */
    public Optional<NormalisedTick> parse(String rawJson, TradingPair tradingPair, long receivedNanos) {
        try {
            Optional<NormalisedTick> result = doParse(rawJson, tradingPair, receivedNanos);
            result.ifPresent(tick -> {
                metrics.recordMessageParsed(ExchangeId.BYBIT);
                metrics.recordParseDuration(ExchangeId.BYBIT,
                        tick.getProcessedTimestamp() - tick.getReceivedTimestamp());
            });
            return result;
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse Bybit message: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.BYBIT, "json_parse");
            return Optional.empty();
        } catch (NumberFormatException e) {
            log.warn("Failed to parse price as BigDecimal: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.BYBIT, "number_format");
            return Optional.empty();
        }
    }

    /**
     * Internal parsing logic separated from error handling for clarity.
     *
     * <p>Handles three message categories:</p>
     * <ul>
     *   <li><b>Pong messages:</b> Heartbeat responses — skip silently (high frequency)</li>
     *   <li><b>Subscription acks:</b> One-time confirmation — skip with debug log</li>
     *   <li><b>Ticker messages:</b> Price data — parse to NormalisedTick</li>
     * </ul>
     *
     * @param rawJson       the raw JSON string
     * @param tradingPair   the trading pair
     * @param receivedNanos T0 timestamp
     * @return the parsed tick, or empty for non-ticker messages
     * @throws JsonProcessingException if the JSON is malformed
     * @throws NumberFormatException   if a price/quantity string is not a valid number
     */
    private Optional<NormalisedTick> doParse(String rawJson, TradingPair tradingPair, long receivedNanos)
            throws JsonProcessingException {

        BybitTickerMessage message = objectMapper.readValue(rawJson, BybitTickerMessage.class);

        // Pong responses to our heartbeat pings — skip without logging (every 20s)
        if (message.isPong()) {
            metrics.recordMessageSkipped(ExchangeId.BYBIT, "pong");
            return Optional.empty();
        }

        // Subscription acks: {"success":true,"op":"subscribe",...}
        if (message.isSubscriptionAck()) {
            log.debug("Bybit subscription acknowledged: {}", truncatePayload(rawJson));
            metrics.recordMessageSkipped(ExchangeId.BYBIT, "subscription_ack");
            return Optional.empty();
        }

        // Not a valid ticker message (missing data or incomplete price fields)
        if (!message.isTickerMessage()) {
            log.debug("Skipping non-ticker Bybit message: {}", truncatePayload(rawJson));
            metrics.recordMessageSkipped(ExchangeId.BYBIT, "non_ticker");
            return Optional.empty();
        }

        BybitTickerMessage.OrderbookData data = message.getData();

        // Derive pair from message symbol; fall back to param in single-pair / test mode
        TradingPair resolvedPair;
        if (symbolLookup.isEmpty()) {
            resolvedPair = tradingPair;
        } else {
            resolvedPair = symbolLookup.get(data.getSymbol());
            if (resolvedPair == null) {
                log.debug("Bybit tick for unconfigured symbol={}, discarding", data.getSymbol());
                metrics.recordMessageSkipped(ExchangeId.BYBIT, "unknown_symbol");
                return Optional.empty();
            }
        }

        // Merge this message with last-known state to handle single-side Bybit deltas.
        // Bybit orderbook.1 deltas include only the side(s) that changed; the other side
        // is absent rather than repeated. Without merging, we would only emit on the rare
        // event that both sides change simultaneously — causing 5–10 second live monitor gaps.
        //
        // Parse BigDecimal values BEFORE mutating state. If a NumberFormatException is thrown
        // (malformed price string), the state array is not partially corrupted — the exception
        // propagates up to parse() which catches it and returns empty.
        BigDecimal newBidPrice    = data.hasBid() ? new BigDecimal(data.bestBidPrice()) : null;
        BigDecimal newBidQuantity = data.hasBid() ? new BigDecimal(data.bestBidSize())  : null;
        BigDecimal newAskPrice    = data.hasAsk() ? new BigDecimal(data.bestAskPrice()) : null;
        BigDecimal newAskQuantity = data.hasAsk() ? new BigDecimal(data.bestAskSize())  : null;

        BigDecimal[] state = lastPrices.computeIfAbsent(data.getSymbol(), k -> new BigDecimal[4]);

        if (newBidPrice != null) {
            state[0] = newBidPrice;
            state[1] = newBidQuantity;
        }
        if (newAskPrice != null) {
            state[2] = newAskPrice;
            state[3] = newAskQuantity;
        }

        // Guard: both sides must be known before emitting. On a fresh connection the
        // very first message is always a snapshot with both sides present, so this
        // branch is only reached during the single-message startup window.
        if (state[0] == null || state[2] == null) {
            log.debug("Bybit tick for symbol={} skipped: awaiting initial snapshot", data.getSymbol());
            metrics.recordMessageSkipped(ExchangeId.BYBIT, "awaiting_snapshot");
            return Optional.empty();
        }

        BigDecimal bestBidPrice    = state[0];
        BigDecimal bestBidQuantity = state[1];
        BigDecimal bestAskPrice    = state[2];
        BigDecimal bestAskQuantity = state[3];

        // T1 — parsing complete, domain object ready to build
        final long processedNanos = System.nanoTime();

        // Bybit provides server timestamp (ts) — use it instead of Instant.now().
        // This enables accurate clock skew measurement between exchange and our system.
        Instant exchangeTimestamp = Instant.ofEpochMilli(message.getTs());

        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(ExchangeId.BYBIT)
                .tradingPair(resolvedPair)
                .bestBidPrice(bestBidPrice)
                .bestAskPrice(bestAskPrice)
                .bestBidQuantity(bestBidQuantity)
                .bestAskQuantity(bestAskQuantity)
                .exchangeTimestamp(exchangeTimestamp)    // Server time — NOT Instant.now()
                .receivedTimestamp(receivedNanos)         // T0 — captured by the connector
                .processedTimestamp(processedNanos)       // T1 — after parsing complete
                .build();

        return Optional.of(tick);
    }

    /**
     * Truncates a payload string for safe logging (avoids flooding logs with large messages).
     *
     * @param payload the raw message payload
     * @return the payload truncated to 200 characters
     */
    private String truncatePayload(String payload) {
        if (payload == null) {
            return "null";
        }
        return payload.length() > 200 ? payload.substring(0, 200) + "..." : payload;
    }
}
