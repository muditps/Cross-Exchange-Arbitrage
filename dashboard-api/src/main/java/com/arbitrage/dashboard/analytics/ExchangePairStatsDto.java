package com.arbitrage.dashboard.analytics;

/**
 * Opportunity statistics grouped by buy-exchange / sell-exchange pair.
 *
 * <p>Used to populate the exchange breakdown bar chart on the Analytics page.
 * The combination {@code buyExchange + sellExchange} identifies a directional
 * arbitrage route (e.g. buy on BINANCE, sell on BYBIT).
 *
 * @param buyExchange      exchange where the buy leg executes (e.g. "BINANCE")
 * @param sellExchange     exchange where the sell leg executes (e.g. "BYBIT")
 * @param count            total opportunities detected for this route
 * @param avgNetSpreadBps  average net spread for this route (bps, 2dp)
 */
public record ExchangePairStatsDto(
        String buyExchange,
        String sellExchange,
        long count,
        double avgNetSpreadBps
) {}
