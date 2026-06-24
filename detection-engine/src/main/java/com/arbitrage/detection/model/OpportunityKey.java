package com.arbitrage.detection.model;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.TradingPair;
import lombok.Value;

/**
 * Composite key that uniquely identifies one arbitrage direction.
 *
 * <p>The key combines (pair, sellExchange, buyExchange). Two ticks represent the
 * "same" opportunity if they share the same key and net spread remains positive.
 * Used as the map key in {@link com.arbitrage.detection.service.OpportunityTracker}.
 *
 * <p><b>Direction matters:</b> (BINANCE sell, KUCOIN buy) and (KUCOIN sell, BINANCE buy)
 * are distinct keys representing opposite market directions. Both can theoretically
 * be profitable simultaneously, though this is extremely rare.
 *
 * <p>{@link lombok.Value} provides correct {@code equals}/{@code hashCode} based on
 * all three fields — required for {@code ConcurrentHashMap} keying.
 */
@Value
public class OpportunityKey {

    /** The trading pair being compared (e.g., BTC-USDT). */
    TradingPair tradingPair;

    /** Exchange where we sell (the exchange with the higher bid). */
    ExchangeId sellExchange;

    /** Exchange where we buy (the exchange with the lower ask). */
    ExchangeId buyExchange;
}
