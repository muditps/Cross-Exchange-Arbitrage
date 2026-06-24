package com.arbitrage.connector.binance;

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
 * Parses raw Binance WebSocket messages into {@link NormalisedTick} objects.
 *
 * <p><b>Responsibility boundary:</b> This class handles <em>data transformation</em> only —
 * JSON string → typed POJO → domain object. It does NOT handle connection lifecycle,
 * T0 timestamp capture, feed status, or sink emission. Those concerns belong to
 * {@link BinanceConnector}.</p>
 *
 * <p><b>Why a separate class?</b> Single Responsibility Principle. The connector changes
 * when the connection protocol changes (WebSocket URL, reconnection strategy). The parser
 * changes when the message format changes (new fields, different JSON structure). Different
 * reasons to change → different classes. This also makes the parser independently testable
 * without mocking WebSocket clients or Reactor sinks.</p>
 *
 * <p><b>Thread safety:</b> This class is stateless — all state is in method parameters.
 * The injected {@link ObjectMapper} is thread-safe by Jackson's contract. Multiple threads
 * can call {@link #parse(String, TradingPair, long)} concurrently with zero synchronisation.</p>
 *
 * <p><b>Latency note:</b> Uses Jackson's full object mapping ({@code readValue}) rather than
 * streaming API ({@code JsonParser}). For Phase 1, this adds ~5-20µs per message. If p99
 * parsing latency exceeds 50µs under load (measured in Phase 6), switch to the streaming API
 * which avoids intermediate object allocation. Profile before optimising.</p>
 *
 * @see BinanceBookTickerMessage for the wire-format POJO
 * @see BinanceConnector for the connection lifecycle that calls this parser
 */
@Component
@Slf4j
public class BinanceMessageParser {

    private final ObjectMapper objectMapper;
    private final ConnectorMetrics metrics;

    /**
     * Lookup map from Binance symbol (e.g. "btcusdt") to canonical TradingPair.
     * Empty in single-pair / test mode — falls back to the tradingPair parameter.
     */
    private final Map<String, TradingPair> symbolLookup;

    /**
     * Single-pair constructor for tests — no symbol lookup, falls back to tradingPair param.
     *
     * @param objectMapper pre-configured Jackson ObjectMapper (singleton)
     * @param metrics      connector metrics
     */
    public BinanceMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics) {
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
    public BinanceMessageParser(ObjectMapper objectMapper, ConnectorMetrics metrics,
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
            // Binance symbol format: lowercase, no separator (BTC-USDT → btcusdt)
            lookup.put((pair.getBaseCurrency() + pair.getQuoteCurrency()).toLowerCase(), pair);
        }
        return lookup;
    }

    /**
     * Parses a raw Binance WebSocket message into a {@link NormalisedTick}.
     *
     * <p>Returns {@link Optional#empty()} for messages that are not bookTicker updates
     * (subscription acks, error messages, unknown formats). This is intentional —
     * non-bookTicker messages are normal protocol traffic, not errors.</p>
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
     * @return the parsed NormalisedTick, or empty if the message is not a bookTicker
     */
    public Optional<NormalisedTick> parse(String rawJson, TradingPair tradingPair, long receivedNanos) {
        try {
            Optional<NormalisedTick> result = doParse(rawJson, tradingPair, receivedNanos);
            result.ifPresent(tick -> {
                metrics.recordMessageParsed(ExchangeId.BINANCE);
                metrics.recordParseDuration(ExchangeId.BINANCE,
                        tick.getProcessedTimestamp() - tick.getReceivedTimestamp());
            });
            return result;
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse Binance message: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.BINANCE, "json_parse");
            return Optional.empty();
        } catch (NumberFormatException e) {
            log.warn("Failed to parse price as BigDecimal: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.BINANCE, "number_format");
            return Optional.empty();
        }
    }

    /**
     * Internal parsing logic separated from error handling for clarity.
     *
     * @param rawJson       the raw JSON string
     * @param tradingPair   the trading pair
     * @param receivedNanos T0 timestamp
     * @return the parsed tick, or empty for non-bookTicker messages
     * @throws JsonProcessingException if the JSON is malformed
     * @throws NumberFormatException   if a price/quantity string is not a valid number
     */
    private Optional<NormalisedTick> doParse(String rawJson, TradingPair tradingPair, long receivedNanos)
            throws JsonProcessingException {

        BinanceBookTickerMessage message = objectMapper.readValue(rawJson, BinanceBookTickerMessage.class);

        // Subscription acks have no price fields: {"result":null,"id":1}
        // @JsonIgnoreProperties means they deserialize with all nulls for price fields
        if (!message.isValid()) {
            log.debug("Skipping non-bookTicker message: {}", truncatePayload(rawJson));
            metrics.recordMessageSkipped(ExchangeId.BINANCE, "non_book_ticker");
            return Optional.empty();
        }

        // Derive pair from message symbol; fall back to param in single-pair / test mode
        TradingPair resolvedPair;
        if (symbolLookup.isEmpty()) {
            resolvedPair = tradingPair;
        } else {
            resolvedPair = symbolLookup.get(message.getSymbol().toLowerCase());
            if (resolvedPair == null) {
                log.debug("Binance tick for unconfigured symbol={}, discarding", message.getSymbol());
                metrics.recordMessageSkipped(ExchangeId.BINANCE, "unknown_symbol");
                return Optional.empty();
            }
        }

        BigDecimal bestBidPrice = new BigDecimal(message.getBestBidPrice());
        BigDecimal bestBidQuantity = new BigDecimal(message.getBestBidQuantity());
        BigDecimal bestAskPrice = new BigDecimal(message.getBestAskPrice());
        BigDecimal bestAskQuantity = new BigDecimal(message.getBestAskQuantity());

        // T1 — parsing complete, domain object ready to build
        final long processedNanos = System.nanoTime();

        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(ExchangeId.BINANCE)
                .tradingPair(resolvedPair)
                .bestBidPrice(bestBidPrice)
                .bestAskPrice(bestAskPrice)
                .bestBidQuantity(bestBidQuantity)
                .bestAskQuantity(bestAskQuantity)
                .exchangeTimestamp(Instant.now())   // bookTicker has no server timestamp
                .receivedTimestamp(receivedNanos)    // T0 — captured by the connector
                .processedTimestamp(processedNanos)  // T1 — after parsing complete
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
