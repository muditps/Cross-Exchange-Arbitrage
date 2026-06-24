package com.arbitrage.detection.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents the current best-bid/ask price state for a single exchange-pair combination.
 *
 * <p>This is the read model for the detection engine: when comparing prices across
 * exchanges, each entry in the cross-exchange price map is a {@link PriceState}.
 * The exchange identity and trading pair are encoded in the Redis key —
 * they are not repeated here to keep the object lean.
 *
 * <p><b>Why a separate value object instead of reusing {@link com.arbitrage.common.model.NormalisedTick}?</b>
 * {@link com.arbitrage.common.model.NormalisedTick} is the hot-path carrier for the Kafka
 * pipeline. {@link PriceState} is the Redis read model for comparison — it omits
 * {@code exchangeId} and {@code tradingPair} (redundant in context), keeping the
 * detection engine's comparison loop working with a purpose-built type rather than
 * reaching into a Kafka-transport object. Separation of concerns.
 *
 * <p><b>All price fields use {@link BigDecimal}.</b> Floating-point arithmetic
 * ({@code double}) cannot represent 0.1 or 0.2 exactly. With crypto prices
 * at 8 decimal places and spreads often at 0.01%, even a {@code 1e-10} rounding
 * error can flip a comparison. {@code BigDecimal} with explicit scale (8) and
 * {@code HALF_UP} rounding prevents this class of bugs entirely.
 *
 * <p><b>Staleness:</b> The {@link #receivedTimestamp} field is a {@code System.nanoTime()}
 * value captured when the WebSocket message arrived at the connector. The detection
 * engine compares {@code currentNanoTime - receivedTimestamp} against the configured
 * staleness threshold to decide whether this price is fresh enough to use.
 */
@Jacksonized
@Value
@Builder
public class PriceState {

    /** Highest price a buyer is willing to pay on this exchange. */
    BigDecimal bestBidPrice;

    /** Lowest price a seller is willing to accept on this exchange. */
    BigDecimal bestAskPrice;

    /** Quantity available at the best bid price. */
    BigDecimal bestBidQuantity;

    /** Quantity available at the best ask price. */
    BigDecimal bestAskQuantity;

    /**
     * Timestamp assigned by the exchange's server.
     *
     * <p><b>Do NOT use this for staleness checks.</b> Exchange clocks may be skewed
     * by hundreds of milliseconds relative to our clock. Use {@link #receivedTimestamp}
     * instead. This field is kept for clock skew monitoring.
     */
    Instant exchangeTimestamp;

    /**
     * {@code System.nanoTime()} captured the instant the WebSocket message arrived —
     * before any parsing. This is the ground truth for staleness checks.
     *
     * <p>A price is stale if {@code System.nanoTime() - receivedTimestamp > stalenessThresholdNs}.
     */
    long receivedTimestamp;

    /**
     * {@code System.nanoTime()} captured after normalisation completed.
     * Used to measure normalisation latency: {@code processedTimestamp - receivedTimestamp}.
     */
    long processedTimestamp;
}
