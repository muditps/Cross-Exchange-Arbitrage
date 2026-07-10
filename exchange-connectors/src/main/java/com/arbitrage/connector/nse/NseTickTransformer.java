package com.arbitrage.connector.nse;

import com.arbitrage.common.model.DataQuality;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Converts a parsed {@link NseQuoteMessage} into a canonical {@link NormalisedTick}.
 *
 * <p><b>Responsibility boundary:</b> This class handles <em>semantic transformation</em> only:
 * instrument token resolution, price unit conversion (paise→INR), and NormalisedTick assembly.
 * JSON deserialization, message type filtering, and metrics recording belong to
 * {@link NseMessageParser}.</p>
 *
 * <p><b>Paise-to-INR conversion:</b> Angel One transmits all prices as integer paise values
 * (1 paise = ₹0.01, so 100 paise = ₹1.00). This is to avoid floating-point wire format.
 * Conversion uses {@code BigDecimal.divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP)}.
 * The {@code HALF_UP} rounding mode is used for safety — paise values should always be
 * exactly divisible by 100 to 2 decimal places, so in practice rounding never fires.
 * If a price is not divisible (future format change), the value is rounded rather than
 * throwing an exception.</p>
 *
 * <p><b>Token resolution:</b> Angel One message responses use numeric instrument tokens
 * ({@code "2885"}) rather than human-readable symbols ({@code "RELIANCE-INR"}). This class
 * maintains a reverse map from token string to {@link TradingPair} built once at startup
 * from {@link NseConnectorProperties#getInstruments()}. If a token is not in the map,
 * the message is discarded (unconfigured instrument).</p>
 *
 * <p><b>Thread safety:</b> The token-to-pair reverse map is built once in the constructor
 * and is never modified after that — safe for concurrent access without synchronization.</p>
 *
 * @see NseMessageParser for the component that calls this transformer
 * @see NseQuoteMessage for the wire-format POJO this transformer consumes
 */
@Component
@Slf4j
public class NseTickTransformer {

    private static final BigDecimal PAISE_PER_RUPEE = BigDecimal.valueOf(100);

    /** Reverse map: Angel One token string → canonical TradingPair. Built once at startup. */
    private final Map<String, TradingPair> tokenToPairMap;

    /**
     * Creates the transformer and builds the token→pair reverse map from configured instruments.
     *
     * @param nseProps NSE connector properties containing the pair→token instrument map
     */
    @Autowired
    public NseTickTransformer(NseConnectorProperties nseProps) {
        this.tokenToPairMap = buildTokenToPairMap(nseProps);
        log.info("NseTickTransformer initialised: {} instrument(s) configured", tokenToPairMap.size());
    }

    /**
     * Package-private constructor for tests — accepts the reverse map directly.
     *
     * @param tokenToPairMap pre-built token→pair map for test injection
     */
    NseTickTransformer(Map<String, TradingPair> tokenToPairMap) {
        this.tokenToPairMap = Map.copyOf(tokenToPairMap);
    }

    /**
     * Transforms a parsed Angel One Quote message into a {@link NormalisedTick}.
     *
     * <p>Returns {@link Optional#empty()} if the instrument token is not configured.
     * This is not an error condition — it means a message arrived for an instrument
     * that is not in the {@code arbitrage.nse.instruments} map in {@code application.yml}.
     * Log at DEBUG, not WARN, to avoid noise from any stray messages.</p>
     *
     * @param message       the parsed Angel One mode-2 Quote message
     * @param receivedNanos T0 timestamp ({@code System.nanoTime()}) captured by {@link NseConnector}
     * @return the normalised tick, or empty if the token is not configured
     */
    public Optional<NormalisedTick> transform(NseQuoteMessage message, long receivedNanos) {
        TradingPair pair = tokenToPairMap.get(message.getToken());
        if (pair == null) {
            log.debug("NSE tick for unconfigured token={}, discarding", message.getToken());
            return Optional.empty();
        }

        BigDecimal bestBidPrice    = paiseToinr(message.getBest5BuyData().get(0).getPrice());
        BigDecimal bestBidQuantity = BigDecimal.valueOf(message.getBest5BuyData().get(0).getQuantity());
        BigDecimal bestAskPrice    = paiseToinr(message.getBest5SellData().get(0).getPrice());
        BigDecimal bestAskQuantity = BigDecimal.valueOf(message.getBest5SellData().get(0).getQuantity());

        final long processedNanos = System.nanoTime();

        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(ExchangeId.NSE)
                .tradingPair(pair)
                .bestBidPrice(bestBidPrice)
                .bestAskPrice(bestAskPrice)
                .bestBidQuantity(bestBidQuantity)
                .bestAskQuantity(bestAskQuantity)
                .exchangeTimestamp(Instant.ofEpochMilli(message.getExchangeTimestamp()))
                .receivedTimestamp(receivedNanos)
                .processedTimestamp(processedNanos)
                .dataQuality(DataQuality.FULL_BOOK)
                .build();

        return Optional.of(tick);
    }

    /**
     * Transforms a parsed LTP-only message into a {@link NormalisedTick} with
     * {@link DataQuality#LTP_ONLY}.
     *
     * <p>The best bid and ask fields are populated with the LTP value as a placeholder.
     * Downstream components that check {@code dataQuality} will skip this tick for spread
     * calculations. The tick is still emitted so it can be displayed on the dashboard.</p>
     *
     * @param message       the parsed Angel One LTP message
     * @param receivedNanos T0 timestamp
     * @return the normalised tick with LTP_ONLY quality, or empty if token is unconfigured
     */
    public Optional<NormalisedTick> transformLtp(NseQuoteMessage message, long receivedNanos) {
        TradingPair pair = tokenToPairMap.get(message.getToken());
        if (pair == null) {
            log.debug("NSE LTP tick for unconfigured token={}, discarding", message.getToken());
            return Optional.empty();
        }

        BigDecimal ltp = paiseToinr(message.getLastTradedPrice());
        final long processedNanos = System.nanoTime();

        NormalisedTick tick = NormalisedTick.builder()
                .exchangeId(ExchangeId.NSE)
                .tradingPair(pair)
                .bestBidPrice(ltp)
                .bestAskPrice(ltp)
                .bestBidQuantity(BigDecimal.ZERO)
                .bestAskQuantity(BigDecimal.ZERO)
                .exchangeTimestamp(Instant.ofEpochMilli(message.getExchangeTimestamp()))
                .receivedTimestamp(receivedNanos)
                .processedTimestamp(processedNanos)
                .dataQuality(DataQuality.LTP_ONLY)
                .build();

        return Optional.of(tick);
    }

    /**
     * Converts a paise integer to a {@link BigDecimal} INR value with 2 decimal places.
     *
     * <p>Angel One prices are always whole paise — e.g., 244900 paise = ₹2449.00.
     * HALF_UP is used for safety; in practice division is always exact.</p>
     *
     * @param paise the price in paise
     * @return the price in INR, scaled to 2 decimal places
     */
    BigDecimal paiseToinr(long paise) {
        return BigDecimal.valueOf(paise).divide(PAISE_PER_RUPEE, 2, RoundingMode.HALF_UP);
    }

    /**
     * Looks up the configured {@link TradingPair} for a given Angel One instrument token.
     * Returns null if the token is not in the configured instrument map.
     *
     * @param token the Angel One instrument token string (e.g., {@code "2885"})
     * @return the trading pair, or null if not configured
     */
    TradingPair resolvePair(String token) {
        return tokenToPairMap.get(token);
    }

    private static Map<String, TradingPair> buildTokenToPairMap(NseConnectorProperties props) {
        Map<String, String> instruments = props.getInstruments();
        if (instruments == null || instruments.isEmpty()) {
            return Map.of();
        }
        Map<String, TradingPair> reverseMap = new HashMap<>();
        for (Map.Entry<String, String> entry : instruments.entrySet()) {
            String pairSymbol = entry.getKey();   // e.g., "RELIANCE-INR"
            String token      = entry.getValue(); // e.g., "2885"
            String[] parts = pairSymbol.split("-", 2);
            if (parts.length == 2) {
                reverseMap.put(token, TradingPair.builder()
                        .baseCurrency(parts[0])
                        .quoteCurrency(parts[1])
                        .build());
            } else {
                log.warn("Skipping malformed NSE instrument entry: symbol={}", pairSymbol);
            }
        }
        return reverseMap;
    }
}
