package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Shared test fixtures and helper builders for transformer tests.
 *
 * <p>Values match the raw fixture files in exchange-connectors so any
 * comparison with parser output is consistent:
 * <ul>
 *   <li>BTC-USDT: bid=67250.50000000, ask=67251.30000000 (Binance fixture 01)</li>
 *   <li>ETH-USDT: bid=3456.78000000, ask=3456.99000000  (KuCoin fixture 03)</li>
 *   <li>SHIB-USDT: bid=0.00002834, ask=0.00002835       (micro-price precision test)</li>
 * </ul>
 */
final class TickTransformerTestSupport {

    private TickTransformerTestSupport() {}

    static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");
    static final TradingPair ETH_USDT = TradingPair.fromSymbol("ETH-USDT");
    static final TradingPair SHIB_USDT = TradingPair.fromSymbol("SHIB-USDT");

    // BTC-USDT prices from exchange connector fixture 01
    static final BigDecimal BTC_BID = new BigDecimal("67250.50000000");
    static final BigDecimal BTC_ASK = new BigDecimal("67251.30000000");
    static final BigDecimal BTC_BID_QTY = new BigDecimal("1.23400000");
    static final BigDecimal BTC_ASK_QTY = new BigDecimal("0.98700000");

    // SHIB-USDT micro-price for precision testing
    static final BigDecimal SHIB_BID = new BigDecimal("0.00002834");
    static final BigDecimal SHIB_ASK = new BigDecimal("0.00002835");
    static final BigDecimal SHIB_QTY = new BigDecimal("1000000.00000000");

    static final long FIXED_RECEIVED_NANOS = 1_000_000_000L;
    static final long FIXED_PROCESSED_NANOS = 1_000_100_000L;
    static final Instant FIXED_EXCHANGE_TIMESTAMP = Instant.ofEpochMilli(1673853746003L);

    /**
     * Builds a valid NormalisedTick for the given exchange with BTC-USDT prices.
     */
    static NormalisedTick buildBtcTick(ExchangeId exchangeId) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(BTC_USDT)
                .bestBidPrice(BTC_BID)
                .bestAskPrice(BTC_ASK)
                .bestBidQuantity(BTC_BID_QTY)
                .bestAskQuantity(BTC_ASK_QTY)
                .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                .receivedTimestamp(FIXED_RECEIVED_NANOS)
                .processedTimestamp(FIXED_PROCESSED_NANOS)
                .build();
    }

    /**
     * Builds a valid NormalisedTick for the given exchange with SHIB-USDT micro-prices.
     * Used for BigDecimal precision testing — verifies tiny values survive pass-through.
     */
    static NormalisedTick buildShibTick(ExchangeId exchangeId) {
        return NormalisedTick.builder()
                .exchangeId(exchangeId)
                .tradingPair(SHIB_USDT)
                .bestBidPrice(SHIB_BID)
                .bestAskPrice(SHIB_ASK)
                .bestBidQuantity(SHIB_QTY)
                .bestAskQuantity(SHIB_QTY)
                .exchangeTimestamp(FIXED_EXCHANGE_TIMESTAMP)
                .receivedTimestamp(FIXED_RECEIVED_NANOS)
                .processedTimestamp(FIXED_PROCESSED_NANOS)
                .build();
    }
}
