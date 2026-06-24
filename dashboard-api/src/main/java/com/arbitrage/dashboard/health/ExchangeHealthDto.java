package com.arbitrage.dashboard.health;

/**
 * Snapshot of one exchange's feed health state, served by {@link HealthController}.
 *
 * <p>The {@code status} field mirrors the {@link com.arbitrage.common.model.FeedStatus}
 * enum name (e.g. "CONNECTED", "STALE", "DISCONNECTED", "RECONNECTING") so the
 * frontend can use it directly without a separate enum lookup.
 *
 * @param exchangeId     exchange identifier (e.g. "BINANCE", "BYBIT", "KUCOIN")
 * @param status         current feed status as a string (FeedStatus enum name)
 * @param lastTickAgeMs  age of the most recent tick in milliseconds; null if no tick
 *                       has been received since startup
 * @param isConnected    true only when status == CONNECTED; convenience field for
 *                       the frontend status dot colour logic
 */
public record ExchangeHealthDto(
        String exchangeId,
        String status,
        Long lastTickAgeMs,
        boolean isConnected
) {}
