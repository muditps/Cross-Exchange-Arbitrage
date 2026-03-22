package com.arbitrage.common.model;

import java.math.BigDecimal;

/**
 * Identifies a supported exchange venue.
 *
 * <p>Each exchange carries its display name and default taker fee rate.
 * Fee rates are stored as {@link BigDecimal} to prevent floating-point
 * rounding errors in spread calculations (e.g., 0.10% = 0.0010).
 *
 * <p>Designed to be extended with additional venues (NSE, BSE) in Phase 7
 * without requiring changes to downstream detection or simulation logic.
 */
public enum ExchangeId {

    BINANCE("Binance", new BigDecimal("0.0010")),
    BYBIT("Bybit", new BigDecimal("0.0010")),
    KUCOIN("KuCoin", new BigDecimal("0.0010"));

    private final String displayName;
    private final BigDecimal defaultTakerFeeRate;

    ExchangeId(String displayName, BigDecimal defaultTakerFeeRate) {
        this.displayName = displayName;
        this.defaultTakerFeeRate = defaultTakerFeeRate;
    }

    public String getDisplayName() {
        return displayName;
    }

    /**
     * Returns the default taker fee rate for this exchange.
     *
     * <p>Taker fees apply when placing market orders (crossing the spread).
     * Our simulation assumes taker orders because speed matters more than
     * fee savings from maker orders. Typical rate: 0.10% (0.0010).
     *
     * <p>This default can be overridden via YAML configuration per exchange.
     *
     * @return fee rate as a decimal (e.g., 0.0010 for 0.10%)
     */
    public BigDecimal getDefaultTakerFeeRate() {
        return defaultTakerFeeRate;
    }
}
