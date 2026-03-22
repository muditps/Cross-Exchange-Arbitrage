package com.arbitrage.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents a detected cross-exchange arbitrage opportunity through its full lifecycle.
 *
 * <p>An opportunity is born when the detection engine finds a positive net spread
 * between two exchanges (after fees). It transitions through states:
 * {@code DETECTED -> OPEN -> CLOSED} (or {@code EXPIRED} if it persists
 * beyond {@code MAX_TRACKING_DURATION}, indicating a likely data artefact).
 *
 * <p><b>Why lifecycle tracking matters:</b> Without it, every tick with a positive
 * spread generates a separate event. At 100ms tick rate across 3 exchanges,
 * that's thousands of duplicate events per minute for the same opportunity.
 * The state machine deduplicates and tracks duration, peak spread, and
 * theoretical profit — far more useful for analysis and backtesting.
 *
 * <p><b>All monetary values use {@link BigDecimal}.</b> See {@link NormalisedTick}
 * for the rationale. Spread values are stored in both absolute amount and
 * basis points (bps) because traders think in bps but accountants think in dollars.
 *
 * <p><b>Pre-funded arbitrage model:</b> We assume balances exist on ALL exchanges
 * simultaneously. Buy on one, sell on another at the same instant. No asset
 * transfer per-trade. Withdrawal fees only apply during periodic rebalancing
 * (daily/weekly), not per-trade. This is how real firms operate.
 */
@Value
@Builder(toBuilder = true)
public class ArbitrageOpportunity {

    /**
     * Unique identifier for this opportunity instance.
     * Used as the key in {@code OpportunityTracker}'s ConcurrentHashMap
     * and as the primary key in TimescaleDB persistence.
     */
    @JsonProperty("id")
    UUID id;

    /** The trading pair where the opportunity was detected (e.g., BTC-USDT). */
    @JsonProperty("tradingPair")
    TradingPair tradingPair;

    // ── Buy side (the exchange where we BUY the asset) ──

    /** Exchange where the asset is cheaper (we buy here). */
    @JsonProperty("buyExchange")
    ExchangeId buyExchange;

    /** Best ask price on the buy exchange at detection time. */
    @JsonProperty("buyPrice")
    BigDecimal buyPrice;

    /** Quantity available at the best ask on the buy exchange. */
    @JsonProperty("buyQuantity")
    BigDecimal buyQuantity;

    /** Taker fee rate applied on the buy exchange (e.g., 0.0010 for 0.10%). */
    @JsonProperty("buyFeeRate")
    BigDecimal buyFeeRate;

    // ── Sell side (the exchange where we SELL the asset) ──

    /** Exchange where the asset is more expensive (we sell here). */
    @JsonProperty("sellExchange")
    ExchangeId sellExchange;

    /** Best bid price on the sell exchange at detection time. */
    @JsonProperty("sellPrice")
    BigDecimal sellPrice;

    /** Quantity available at the best bid on the sell exchange. */
    @JsonProperty("sellQuantity")
    BigDecimal sellQuantity;

    /** Taker fee rate applied on the sell exchange (e.g., 0.0010 for 0.10%). */
    @JsonProperty("sellFeeRate")
    BigDecimal sellFeeRate;

    // ── Spread calculations ──

    /**
     * Gross spread before fees: {@code sellPrice - buyPrice}.
     * A positive value means the sell exchange quotes higher than the buy exchange.
     */
    @JsonProperty("grossSpread")
    BigDecimal grossSpread;

    /**
     * Net spread after deducting fees on both legs:
     * {@code grossSpread - (buyPrice * buyFeeRate) - (sellPrice * sellFeeRate)}.
     *
     * <p>Only opportunities with {@code netSpread > 0} are truly profitable.
     * Most detected "opportunities" on major pairs will have negative net spreads
     * because fees ($134 round-trip on BTC) dwarf typical spreads ($15).
     */
    @JsonProperty("netSpread")
    BigDecimal netSpread;

    /**
     * Gross spread expressed in basis points: {@code (grossSpread / buyPrice) * 10000}.
     * Traders think in bps — a 5 bps spread is more intuitive than "$3.35."
     */
    @JsonProperty("grossSpreadBps")
    BigDecimal grossSpreadBps;

    /** Net spread expressed in basis points. */
    @JsonProperty("netSpreadBps")
    BigDecimal netSpreadBps;

    // ── Quantity and profit ──

    /**
     * The maximum quantity that can be arbitraged: {@code min(buyQuantity, sellQuantity)}.
     * Profit is bounded by the thinner side of the order book.
     */
    @JsonProperty("arbitrageableQuantity")
    BigDecimal arbitrageableQuantity;

    /**
     * Theoretical profit if both legs execute at quoted prices:
     * {@code netSpread * arbitrageableQuantity}.
     *
     * <p>"Theoretical" because real execution faces slippage, partial fills,
     * and price movement during the execution window. The execution simulator
     * (Phase 4) models these realities.
     */
    @JsonProperty("theoreticalProfit")
    BigDecimal theoreticalProfit;

    // ── Lifecycle state ──

    /** Current lifecycle state of this opportunity. */
    @JsonProperty("status")
    OpportunityStatus status;

    // ── Timestamps ──

    /** When the opportunity was first detected (wall-clock for persistence). */
    @JsonProperty("detectionTimestamp")
    Instant detectionTimestamp;

    /** When the opportunity was last updated (status change or spread update). */
    @JsonProperty("lastUpdateTimestamp")
    Instant lastUpdateTimestamp;

    /**
     * {@code System.nanoTime()} at first detection.
     * Used with {@code closedNanoTime} to compute precise duration
     * without wall-clock jumps from NTP sync.
     */
    @JsonProperty("detectedNanoTime")
    long detectedNanoTime;

    /**
     * {@code System.nanoTime()} when the opportunity closed or expired.
     * Zero if still open.
     */
    @JsonProperty("closedNanoTime")
    long closedNanoTime;

    // ── Tracking metrics (updated while OPEN) ──

    /**
     * Highest net spread observed during this opportunity's lifetime.
     * Useful for analysing "how good was this opportunity at its best?"
     */
    @JsonProperty("peakNetSpread")
    BigDecimal peakNetSpread;

    /**
     * Running average of net spread samples while the opportunity was open.
     * Updated on each tick where the opportunity remains profitable.
     */
    @JsonProperty("averageNetSpread")
    BigDecimal averageNetSpread;

    /**
     * Total duration in milliseconds from DETECTED to CLOSED/EXPIRED.
     * Computed from nanoTime difference for monotonic accuracy.
     * Zero if still open.
     */
    @JsonProperty("totalDurationMs")
    long totalDurationMs;
}
