package com.arbitrage.simulator.model;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.TradingPair;

/**
 * Composite key for the {@link com.arbitrage.simulator.service.HistoricalPriceStore}.
 *
 * <p>Ticks are bucketed by both exchange and trading pair because price history
 * is exchange-specific: Binance BTC-USDT and Bybit BTC-USDT are independent
 * order books with different prices. Combining them under a single key would
 * corrupt lookup results by mixing prices from different sources.
 *
 * <p>A Java record is used here instead of a Lombok {@code @Value} class for
 * two reasons:
 * <ol>
 *   <li>{@code record} auto-generates structurally correct {@code equals()} and
 *       {@code hashCode()} by component value — required for correct
 *       {@link java.util.concurrent.ConcurrentHashMap} behaviour.</li>
 *   <li>The declaration is 1 line vs 10+ for a Lombok class. Records are
 *       idiomatic Java 16+ for pure data carriers.</li>
 * </ol>
 *
 * @param exchangeId  the exchange that produced the tick (e.g., BINANCE)
 * @param tradingPair the canonical trading pair (e.g., BTC-USDT)
 */
public record PriceKey(ExchangeId exchangeId, TradingPair tradingPair) {
}
