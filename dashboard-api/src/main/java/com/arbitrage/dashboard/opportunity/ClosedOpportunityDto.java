package com.arbitrage.dashboard.opportunity;

/**
 * A closed or expired arbitrage opportunity read from the {@code arbitrage_opportunities}
 * TimescaleDB table. Used for the historical feed table and detail modal in the dashboard.
 *
 * <p>All price and spread fields are {@code String} to preserve {@code NUMERIC(20,8)} precision
 * from TimescaleDB without loss through Java {@code double} or JavaScript {@code number}.
 *
 * <p>Column name mapping differs slightly from the Java domain model:
 * <ul>
 *   <li>DB {@code raw_spread_bps} → {@code rawSpreadBps} (domain model calls it gross)</li>
 *   <li>DB {@code quantity} → {@code quantity} (domain model: arbitrageableQuantity)</li>
 *   <li>DB {@code estimated_profit} → {@code estimatedProfit} (domain model: theoreticalProfit)</li>
 * </ul>
 *
 * @param id                   UUID string of the opportunity
 * @param tradingPair          canonical pair symbol (e.g. "BTC-USDT")
 * @param buyExchange          exchange where the buy leg executes
 * @param sellExchange         exchange where the sell leg executes
 * @param buyPrice             ask price on buy exchange at detection (8dp string)
 * @param sellPrice            bid price on sell exchange at detection (8dp string)
 * @param rawSpreadBps         gross spread before fees in basis points
 * @param netSpreadBps         spread after deducting both taker fees
 * @param buyFeeRate           taker fee rate on buy exchange (e.g. "0.001000")
 * @param sellFeeRate          taker fee rate on sell exchange
 * @param quantity             min(buyAskQty, sellBidQty) — arbitrageable volume
 * @param estimatedProfit      netSpread * quantity in quote currency
 * @param status               CLOSED or EXPIRED
 * @param detectionTimestamp   ISO-8601 UTC when first detected
 * @param closedTimestamp      ISO-8601 UTC when opportunity ended (null if still open)
 * @param peakNetSpreadBps     highest net spread observed during the opportunity's lifetime
 * @param averageNetSpreadBps  running average spread across all observations
 * @param totalDurationMs      milliseconds from detection to close
 */
public record ClosedOpportunityDto(
        String id,
        String tradingPair,
        String buyExchange,
        String sellExchange,
        String buyPrice,
        String sellPrice,
        String rawSpreadBps,
        String netSpreadBps,
        String buyFeeRate,
        String sellFeeRate,
        String quantity,
        String estimatedProfit,
        String status,
        String detectionTimestamp,
        String closedTimestamp,
        String peakNetSpreadBps,
        String averageNetSpreadBps,
        Long totalDurationMs
) {}
