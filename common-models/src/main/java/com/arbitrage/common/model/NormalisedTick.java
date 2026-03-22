package com.arbitrage.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The universal tick schema used by every module downstream of normalisation.
 *
 * <p>Each exchange sends data in a completely different format (field names,
 * nesting, timestamp precision). The normalisation engine absorbs all of
 * those quirks and produces this unified object. The detection engine,
 * execution simulator, and dashboard never need to know which exchange
 * produced the tick — they only see {@code NormalisedTick}.
 *
 * <p><b>Hot path object.</b> This is on the critical path from tick reception
 * to arbitrage detection. It is immutable ({@code @Value}) to eliminate
 * synchronization overhead — no thread can corrupt an object that cannot
 * be modified. Consider switching to a Java record in Phase 6 if GC
 * pressure from builder allocations is observed.
 *
 * <p><b>All prices use {@link BigDecimal}.</b> Floating-point arithmetic
 * ({@code double}) causes rounding errors: {@code 0.1 + 0.2 != 0.3}.
 * With crypto prices at 8 decimal places and spreads often at 0.01%,
 * even a {@code 1e-10} rounding error can flip a comparison.
 * {@code BigDecimal} with explicit scale (8) and {@code HALF_UP} rounding
 * prevents this class of bugs entirely.
 *
 * <p><b>Three timestamps:</b>
 * <ul>
 *   <li>{@code exchangeTimestamp} — the exchange's server clock (may be skewed)</li>
 *   <li>{@code receivedTimestamp} — {@code System.nanoTime()} at WebSocket message
 *       arrival (our clock, used for staleness checks)</li>
 *   <li>{@code processedTimestamp} — {@code System.nanoTime()} after normalisation
 *       completes</li>
 * </ul>
 */
@Value
@Builder
public class NormalisedTick {

    /** Which exchange produced this tick. */
    @JsonProperty("exchangeId")
    ExchangeId exchangeId;

    /** The trading pair in canonical format (e.g., BTC-USDT). */
    @JsonProperty("tradingPair")
    TradingPair tradingPair;

    /** Highest price a buyer is willing to pay on this exchange. */
    @JsonProperty("bestBidPrice")
    BigDecimal bestBidPrice;

    /** Lowest price a seller is willing to accept on this exchange. */
    @JsonProperty("bestAskPrice")
    BigDecimal bestAskPrice;

    /** Quantity available at the best bid price. */
    @JsonProperty("bestBidQuantity")
    BigDecimal bestBidQuantity;

    /** Quantity available at the best ask price. */
    @JsonProperty("bestAskQuantity")
    BigDecimal bestAskQuantity;

    /**
     * Timestamp assigned by the exchange's server.
     *
     * <p><b>Do NOT use this for staleness checks.</b> Exchange clocks may be
     * skewed by hundreds of milliseconds relative to our clock. Use
     * {@link #receivedTimestamp} instead. This field is kept for
     * clock skew monitoring and within-exchange ordering.
     */
    @JsonProperty("exchangeTimestamp")
    Instant exchangeTimestamp;

    /**
     * {@code System.nanoTime()} captured the instant the WebSocket message
     * arrives at our application — before any parsing or processing.
     *
     * <p><b>This is the ground truth for staleness.</b> A tick is stale if
     * {@code currentNanoTime - receivedTimestamp > STALENESS_THRESHOLD_NS}.
     * Using a monotonic clock (nanoTime) avoids wall-clock jumps from NTP sync.
     */
    @JsonProperty("receivedTimestamp")
    long receivedTimestamp;

    /**
     * {@code System.nanoTime()} captured after normalisation completes.
     * Used to measure normalisation latency: {@code processedTimestamp - receivedTimestamp}.
     */
    @JsonProperty("processedTimestamp")
    long processedTimestamp;
}
