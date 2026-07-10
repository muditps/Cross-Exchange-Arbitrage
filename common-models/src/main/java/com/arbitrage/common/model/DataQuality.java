package com.arbitrage.common.model;

/**
 * Indicates the quality of price data available in a {@link NormalisedTick}.
 *
 * <p>Not all exchanges and subscription modes provide both bid and ask prices.
 * Angel One SmartAPI mode-1 (LTP) provides only the last traded price — useful
 * for trend monitoring but insufficient for arbitrage detection, which requires
 * a live spread. Mode-2 (Quote) provides best bid and best ask, enabling full
 * spread calculation.
 *
 * <p><b>Downstream impact:</b> The detection engine should skip ticks with
 * {@code LTP_ONLY} quality for arbitrage comparison — a last traded price is
 * not an actionable bid or ask. It may still be used for informational display
 * on the dashboard.
 *
 * <p><b>Default for crypto connectors:</b> Binance, Bybit, and KuCoin all
 * provide live order book data (best bid + best ask) — they always produce
 * {@code FULL_BOOK} ticks.
 *
 * <p><b>NSE/BSE:</b> The NSE connector subscribes to mode-2 (Quote), which
 * provides {@code FULL_BOOK} data. If a mode-1 subscription is ever used
 * (e.g., for lower-bandwidth instruments), the parser sets {@code LTP_ONLY}.
 */
public enum DataQuality {

    /**
     * Both best bid price and best ask price are available.
     * The tick can be used for spread calculation and arbitrage detection.
     */
    FULL_BOOK,

    /**
     * Only the last traded price is available; no live bid or ask.
     * The tick cannot be used for arbitrage spread calculation.
     * Suitable for display/charting only.
     */
    LTP_ONLY
}
