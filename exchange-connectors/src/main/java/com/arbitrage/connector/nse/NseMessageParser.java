package com.arbitrage.connector.nse;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Parses raw Angel One SmartStream WebSocket messages into {@link NormalisedTick} objects.
 *
 * <p><b>Responsibility boundary:</b> This class handles JSON deserialization, message type
 * filtering, error handling, and metrics. Price conversion and domain object construction
 * are delegated to {@link NseTickTransformer} (Single Responsibility Principle — parser
 * changes when Angel One changes their JSON format; transformer changes when conversion
 * logic changes). The two classes have different reasons to change.</p>
 *
 * <p><b>Message type handling:</b>
 * <ul>
 *   <li><b>{@code quote_data}</b> — mode-2 Quote with best-5 bid/ask levels. Delegated to
 *       {@link NseTickTransformer#transform}. Produces {@code FULL_BOOK} tick.</li>
 *   <li><b>{@code ltp_data}</b> — mode-1 LTP (last traded price only). Delegated to
 *       {@link NseTickTransformer#transformLtp}. Produces {@code LTP_ONLY} tick.</li>
 *   <li><b>Everything else</b> — subscription acks, error frames, unknown. Skipped at DEBUG.</li>
 * </ul>
 *
 * <p><b>Error handling strategy:</b> Malformed JSON and unexpected message structures are
 * logged at WARN and return {@code Optional.empty()}. Exceptions are never propagated — in a
 * reactive stream, an unhandled exception terminates the entire subscriber chain. One bad
 * message must not kill the NSE feed.</p>
 *
 * <p><b>Wire format (mode-2 Quote):</b>
 * <pre>{@code
 * {
 *   "type": "quote_data",
 *   "token": "2885",
 *   "exchange_timestamp": 1688000000000,
 *   "last_traded_price": 245000,
 *   "subscription_mode": 2,
 *   "best_5_buy_data":  [{"price": 244900, "quantity": 100, "no_of_orders": 5}],
 *   "best_5_sell_data": [{"price": 245100, "quantity": 200, "no_of_orders": 3}]
 * }
 * }</pre>
 * All prices are integers in paise (₹1 = 100 paise). {@link NseTickTransformer} divides by 100.
 *
 * @see NseConnector for the connector that captures T0 and calls this parser
 * @see NseTickTransformer for paise-to-INR conversion and NormalisedTick assembly
 * @see NseQuoteMessage for the wire-format POJO
 */
@Component
@Slf4j
public class NseMessageParser {

    private final ObjectMapper objectMapper;
    private final ConnectorMetrics metrics;
    private final NseTickTransformer transformer;

    /**
     * Creates the NSE message parser.
     *
     * @param objectMapper pre-configured singleton Jackson ObjectMapper
     * @param metrics      connector metrics for parse outcome recording
     * @param transformer  handles token resolution, paise→INR conversion, and NormalisedTick assembly
     */
    @Autowired
    public NseMessageParser(ObjectMapper objectMapper,
                            ConnectorMetrics metrics,
                            NseTickTransformer transformer) {
        this.objectMapper = objectMapper;
        this.metrics      = metrics;
        this.transformer  = transformer;
    }

    /**
     * Parses a raw Angel One SmartStream message into a {@link NormalisedTick}.
     *
     * <p>Returns {@link Optional#empty()} for all non-market-data messages (subscription acks,
     * pong responses, error frames) and for messages with an unconfigured instrument token.
     * Never throws — exceptions are caught, logged, and converted to empty.</p>
     *
     * @param rawJson       the raw JSON string received from the Angel One WebSocket
     * @param tradingPair   unused for NSE (token→pair lookup is done by the transformer);
     *                      kept for interface consistency with other parsers
     * @param receivedNanos T0 timestamp ({@code System.nanoTime()}) captured by {@link NseConnector}
     *                      as the absolute first operation on message arrival
     * @return the parsed normalised tick, or empty if the message is not actionable market data
     */
    public Optional<NormalisedTick> parse(String rawJson, TradingPair tradingPair, long receivedNanos) {
        try {
            Optional<NormalisedTick> result = doParse(rawJson, receivedNanos);
            result.ifPresent(tick -> {
                metrics.recordMessageParsed(ExchangeId.NSE);
                metrics.recordParseDuration(ExchangeId.NSE,
                        tick.getProcessedTimestamp() - tick.getReceivedTimestamp());
            });
            return result;
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse NSE message: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.NSE, "json_parse");
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Unexpected error parsing NSE message: error={} payload={}",
                    e.getMessage(), truncatePayload(rawJson));
            metrics.recordParseError(ExchangeId.NSE, "unexpected");
            return Optional.empty();
        }
    }

    private Optional<NormalisedTick> doParse(String rawJson, long receivedNanos)
            throws JsonProcessingException {

        NseQuoteMessage message = objectMapper.readValue(rawJson, NseQuoteMessage.class);

        if (message.isQuoteData()) {
            return transformer.transform(message, receivedNanos);
        }

        if (message.isLtpData()) {
            log.debug("NSE LTP-only message received: token={}", message.getToken());
            metrics.recordMessageReceived(ExchangeId.NSE);
            return transformer.transformLtp(message, receivedNanos);
        }

        // Subscription ack, error frame, or unknown type — discard silently at DEBUG
        log.debug("Skipping non-market-data NSE message: type={}", message.getType());
        metrics.recordMessageSkipped(ExchangeId.NSE, "non_market_data");
        return Optional.empty();
    }

    private String truncatePayload(String payload) {
        if (payload == null) {
            return "null";
        }
        return payload.length() > 200 ? payload.substring(0, 200) + "..." : payload;
    }
}
