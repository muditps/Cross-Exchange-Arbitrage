package com.arbitrage.common.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.Objects;

/**
 * Represents a tradeable asset pair in a canonical, exchange-agnostic format.
 *
 * <p>Examples: {@code BTC-USDT}, {@code ETH-BTC}, {@code RELIANCE-INR}.
 * The canonical format uses a hyphen separator and uppercase symbols,
 * regardless of how individual exchanges represent the pair
 * (Binance: {@code BTCUSDT}, KuCoin: {@code BTC-USDT}, NSE: {@code RELIANCE}).
 *
 * <p>This is an immutable value object — safe to use as a HashMap key,
 * pass across threads, and include in Kafka message keys.
 */
@Value
@Builder
@Jacksonized
public class TradingPair {

    /**
     * The asset being bought or sold (e.g., "BTC", "ETH", "RELIANCE").
     */
    String baseCurrency;

    /**
     * The asset used to price and settle (e.g., "USDT", "BTC", "INR").
     */
    String quoteCurrency;

    /**
     * Returns the canonical symbol in {@code "BASE-QUOTE"} format.
     *
     * <p>This is the universal identifier used across all modules:
     * Kafka message keys, Redis key suffixes, database columns,
     * and dashboard display. Every exchange's native format is
     * mapped to this canonical form during normalisation.
     *
     * @return canonical symbol (e.g., "BTC-USDT", "RELIANCE-INR")
     */
    public String canonicalSymbol() {
        return baseCurrency + "-" + quoteCurrency;
    }

    /**
     * Creates a {@link TradingPair} from a canonical symbol string.
     *
     * @param symbol the canonical symbol (e.g., "BTC-USDT")
     * @return parsed TradingPair
     * @throws IllegalArgumentException if the symbol format is invalid
     */
    public static TradingPair fromSymbol(String symbol) {
        Objects.requireNonNull(symbol, "Symbol must not be null");
        String[] parts = symbol.split("-");
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException(
                    "Invalid trading pair symbol: '" + symbol + "'. Expected format: BASE-QUOTE (e.g., BTC-USDT)");
        }
        return TradingPair.builder()
                .baseCurrency(parts[0].toUpperCase())
                .quoteCurrency(parts[1].toUpperCase())
                .build();
    }

    @Override
    public String toString() {
        return canonicalSymbol();
    }
}
