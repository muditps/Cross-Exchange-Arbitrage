package com.arbitrage.connector.kucoin;

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

/**
 * Parses raw KuCoin WebSocket messages into {@link NormalisedTick} objects.
 *
 * <p><b>Responsibility boundary:</b> This class handles <em>data transformation</em> only —
 * JSON string → typed POJO → domain object. Connection lifecycle, T0 timestamp capture,
 * heartbeat pings, feed status, and sink emission all belong to {@link KuCoinConnector}.</p>
 *
 * <p><b>Message type handling:</b> KuCoin sends four message types: welcome (on connect),
 * ack (subscription confirmation), pong (heartbeat response), and message (ticker data).
 * Only {@code "message"} type with a valid {@code data} object produces a NormalisedTick.
 * All other types are logged at appropriate levels and return {@link Optional#empty()}.</p>
 *
 * <p><b>Welcome message:</b> KuCoin sends a welcome message immediately upon connection
 * ({@code {"id":"...","type":"welcome"}}). This is important for connection health — if the
 * connector does NOT receive a welcome, it means the WebSocket URL or token was rejected.
 * We log it at INFO level (unlike pongs which are logged at DEBUG) so connection establishment
 * is visible in production logs.</p>
 *
 * <p><b>Exchange timestamp:</b> KuCoin provides a millisecond server timestamp in
 * {@code data.time}. This is used for {@code exchangeTimestamp} in NormalisedTick via
 * {@code Instant.ofEpochMilli(data.getTime())}. This is consistent with the Bybit connector's
 * use of the server {@code ts} field — both are strictly better than {@code Instant.now()}
 * because they reflect exchange-side data generation time rather than our receive time.</p>
 *
 * <p><b>Thread safety:</b> This class is stateless — all state is in method parameters.
 * The injected {@link ObjectMapper} is thread-safe by Jackson's contract. Multiple threads
 * can call {@link #parse} concurrently with zero synchronisation.</p>
 *
 * @see KuCoinTickerMessage for the wire-format POJO
 * @see KuCoinConnector for the connection lifecycle that calls this parser
 */
@Component
@Slf4j
public class KuCoinMessageParser {

    private static final String TOPIC_PREFIX = "/market/ticker:";

    private final ObjectMapper objectMapper;
    private final ConnectorMetrics metrics;

    /**
     * Lookup map from canonical KuCoin symbol (e.g. "BTC-USDT") to TradingPair.
     * KuCoin sends the canonical format in the topic field — no conversion needed.
     * Empty in single-pair / test mode — falls back to the tradingPair parameter.
     */
    private final Map<String, TradingPair> symbolLookup;

    /**
     * Single-pair constructor for tests — no symbol lookup, falls back to tradingPair param.
     *
     * @param objectMapper pre-configured Jackson ObjectMapper (singleton)
     * @param metrics      connector metrics
     */
    public KuCoinMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics) {
        this(objectMapper, metrics, null);
    }

    /**
     * Multi-pair constructor used by Spring. Builds a symbol→pair lookup from all configured pairs.
     *
     * @param objectMapper          pre-configured Jackson ObjectMapper (singleton, thread-safe)
     * @param metrics               connector metrics for recording parse outcomes and latency
     * @param tradingPairsProperties configured trading pairs; null produces empty lookup (test mode)
     */
    @Autowired
    public KuCoinMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics,
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
            // KuCoin uses canonical format (BTC-USDT) — matches canonicalSymbol() directly
            lookup.put(pair.canonicalSymbol(), pair);
        }
        return lookup;
    }

    /**
     * Parses a raw KuCoin WebSocket message into a {@link NormalisedTick}.
     *
     * <p>Returns {@link Optional#empty()} for non-ticker messages (welcome, ack, pong)
     * and for malformed or incomplete ticker messages. Does NOT throw exceptions — in a
     * hot reactive stream, an uncaught exception terminates all subscribers. One bad
     * message must not kill the feed.</p>
     *
     * @param rawJson       the raw JSON string from the WebSocket
     * @param tradingPair   the trading pair this message belongs to
     * @param receivedNanos T0 timestamp ({@code System.nanoTime()} captured by the connector
     *                      as the very first operation on message arrival)
     * @return the parsed NormalisedTick, or empty if the message is not a ticker update
     */
    public Optional<NormalisedTick> parse(String rawJson, TradingPair tradingPair, long receivedNanos) {
        try {
            Optional<NormalisedTick> result = doParse(rawJson, tradingPair, receivedNanos);
            result.ifPresent(tick -> {
                metrics.recordMessageParsed(ExchangeId.KUCOIN);
                metrics.recordParseDuration(ExchangeId.KUCOIN,
                        tick.getProcessedTimestamp() - tick.getReceivedTimestamp());
            });
            return result;
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse KuCoin message: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.KUCOIN, "json_parse");
            return Optional.empty();
        } catch (NumberFormatException e) {
            log.warn("Failed to parse KuCoin price as BigDecimal: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.KUCOIN, "number_format");
            return Optional.empty();
        }
    }

    /**
     * Internal parsing logic separated from error handling.
     *
     * <p>Handles all four KuCoin message types:</p>
     * <ul>
     *   <li><b>Welcome:</b> Logged at INFO — confirms connection bootstrap succeeded</li>
     *   <li><b>Pong:</b> Logged at TRACE — heartbeat response, very frequent</li>
     *   <li><b>Ack:</b> Logged at DEBUG — one-time subscription confirmation</li>
     *   <li><b>Message:</b> Parsed to NormalisedTick</li>
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

        KuCoinTickerMessage message = objectMapper.readValue(rawJson, KuCoinTickerMessage.class);

        // Welcome message — confirms the WebSocket URL and token were accepted.
        // Logged at INFO because absence of this message indicates a bootstrap failure.
        if (message.isWelcome()) {
            log.info("KuCoin WebSocket connection accepted (welcome received): id={}", message.getId());
            metrics.recordMessageSkipped(ExchangeId.KUCOIN, "welcome");
            return Optional.empty();
        }

        // Pong responses — logged at TRACE to avoid flooding logs (every 18s)
        if (message.isPong()) {
            log.trace("KuCoin pong received: id={}", message.getId());
            metrics.recordMessageSkipped(ExchangeId.KUCOIN, "pong");
            return Optional.empty();
        }

        // Subscription acknowledgement — one-time confirmation per subscribe
        if (message.isAck()) {
            log.debug("KuCoin subscription acknowledged: id={}", message.getId());
            metrics.recordMessageSkipped(ExchangeId.KUCOIN, "ack");
            return Optional.empty();
        }

        // Not a valid ticker message (missing data or incomplete price fields)
        if (!message.isTickerMessage()) {
            log.debug("Skipping non-ticker KuCoin message: type={} payload={}",
                    message.getType(), truncatePayload(rawJson));
            metrics.recordMessageSkipped(ExchangeId.KUCOIN, "non_ticker");
            return Optional.empty();
        }

        KuCoinTickerMessage.TickerData data = message.getData();

        // Derive pair from topic string (e.g. "/market/ticker:BTC-USDT" → "BTC-USDT")
        TradingPair resolvedPair;
        String topic = message.getTopic();
        if (symbolLookup.isEmpty()) {
            resolvedPair = tradingPair; // test/single-pair mode
        } else if (topic != null && topic.startsWith(TOPIC_PREFIX)) {
            String symbol = topic.substring(TOPIC_PREFIX.length());
            resolvedPair = symbolLookup.get(symbol);
            if (resolvedPair == null) {
                log.debug("KuCoin tick for unconfigured symbol={}, discarding", symbol);
                metrics.recordMessageSkipped(ExchangeId.KUCOIN, "unknown_symbol");
                return Optional.empty();
            }
        } else {
            resolvedPair = tradingPair;
        }

        BigDecimal bestBidPrice = new BigDecimal(data.getBestBid());
        BigDecimal bestBidQuantity = new BigDecimal(data.getBestBidSize());
        BigDecimal bestAskPrice = new BigDecimal(data.getBestAsk());
        BigDecimal bestAskQuantity = new BigDecimal(data.getBestAskSize());

        // T1 — parsing complete, domain object ready to build
        final long processedNanos = System.nanoTime();

        // KuCoin provides server timestamp in data.time (epoch milliseconds).
        // Use it for exchangeTimestamp instead of Instant.now() — same approach as Bybit.
        Instant exchangeTimestamp = Instant.ofEpochMilli(data.getTime());

        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(ExchangeId.KUCOIN)
                .tradingPair(resolvedPair)
                .bestBidPrice(bestBidPrice)
                .bestAskPrice(bestAskPrice)
                .bestBidQuantity(bestBidQuantity)
                .bestAskQuantity(bestAskQuantity)
                .exchangeTimestamp(exchangeTimestamp)   // Server time — NOT Instant.now()
                .receivedTimestamp(receivedNanos)        // T0 — captured by the connector
                .processedTimestamp(processedNanos)      // T1 — after parsing complete
                .build();

        return Optional.of(tick);
    }

    /**
     * Truncates a payload string to 200 characters for safe, concise logging.
     *
     * @param payload the raw message payload
     * @return the payload truncated to 200 characters, or "null" if null
     */
    private String truncatePayload(String payload) {
        if (payload == null) {
            return "null";
        }
        return payload.length() > 200 ? payload.substring(0, 200) + "..." : payload;
    }
}
