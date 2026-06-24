package com.arbitrage.detection;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Factory for synthetic {@link NormalisedTick} instances used in integration tests.
 *
 * <p>All required fields are populated so the detection engine accepts ticks without
 * rejection. Two factory methods cover the two scenarios needed by integration tests:
 * a fresh tick (non-stale) and a deliberately aged tick (stale beyond any threshold).
 */
public final class TestTickGenerator {

    private static final BigDecimal DEFAULT_QUANTITY = new BigDecimal("1.00000000");

    private TestTickGenerator() {}

    /**
     * Builds a fresh tick whose {@code receivedTimestamp} is the current {@code System.nanoTime()}.
     *
     * <p>The tick is non-stale for any staleness threshold above ~1ms. Use this for all
     * normal test scenarios (profitable spread, net-negative spread, lifecycle tests).
     *
     * @param exchange the exchange this tick originates from
     * @param pair     the trading pair
     * @param bidPrice best bid price as a decimal string (e.g., "51000.00")
     * @param askPrice best ask price as a decimal string (e.g., "51001.00")
     * @return a fully populated NormalisedTick ready for the normalised-ticks topic
     */
    public static NormalisedTick freshTick(ExchangeId exchange, TradingPair pair,
                                           String bidPrice, String askPrice) {
        return NormalisedTick.builder()
                .exchangeId(exchange)
                .tradingPair(pair)
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(askPrice))
                .bestBidQuantity(DEFAULT_QUANTITY)
                .bestAskQuantity(DEFAULT_QUANTITY)
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }

    /**
     * Builds a stale tick whose {@code receivedTimestamp} is {@code ageSeconds} in the past.
     *
     * <p>The tick's price is well-formed and would be profitable if fresh. The only
     * thing that makes it stale is the aged {@code receivedTimestamp}. Use this to verify
     * that {@link com.arbitrage.detection.service.StalenessFilter} rejects old prices
     * before any spread arithmetic runs.
     *
     * @param exchange   the exchange this tick originates from
     * @param pair       the trading pair
     * @param bidPrice   best bid price as a decimal string
     * @param askPrice   best ask price as a decimal string
     * @param ageSeconds how many seconds in the past to backdate the receivedTimestamp
     * @return a fully populated NormalisedTick with a deliberately aged receivedTimestamp
     */
    public static NormalisedTick staleTick(ExchangeId exchange, TradingPair pair,
                                           String bidPrice, String askPrice, long ageSeconds) {
        long staleNanoTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(ageSeconds);
        return NormalisedTick.builder()
                .exchangeId(exchange)
                .tradingPair(pair)
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(askPrice))
                .bestBidQuantity(DEFAULT_QUANTITY)
                .bestAskQuantity(DEFAULT_QUANTITY)
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(staleNanoTime)
                .processedTimestamp(System.nanoTime())
                .build();
    }
}
