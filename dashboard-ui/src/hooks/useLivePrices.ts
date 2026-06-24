import { useMemo } from 'react';
import type { ExchangeId, WsConnectionStatus } from '@/types';
import { canonicalSymbol } from '@/types';
import { useArbitrageStore, type LivePrice } from '@/stores/arbitrageStore';

// Re-export so callers don't need to import from two places
export type { LivePrice } from '@/stores/arbitrageStore';

/** Per-exchange price state for a single trading pair. */
export type LivePriceMap = Partial<Record<ExchangeId, LivePrice>>;

/**
 * Returns a per-exchange price map for the requested trading pair, derived from
 * the `livePrices` store field written by {@link WebSocketProvider}.
 *
 * The WS connection is NOT managed here — it lives in WebSocketProvider at App level
 * so it persists across page navigation. This hook is a pure read from store state.
 *
 * The store holds all exchange+pair slots in a flat map keyed by "EXCHANGE:PAIR".
 * This hook filters that map for entries matching `tradingPair` (O(N) where N ≤ 9
 * for 3 exchanges × 3 pairs), then presents them keyed by ExchangeId for the UI.
 *
 * @param tradingPair Canonical symbol to filter for (e.g. "BTC-USDT")
 */
export function useLivePrices(tradingPair: string): {
  prices: LivePriceMap;
  wsStatus: WsConnectionStatus;
} {
  const livePrices = useArbitrageStore((s) => s.livePrices);
  const wsStatus = useArbitrageStore((s) => s.wsStatus);

  const prices = useMemo<LivePriceMap>(() => {
    const result: LivePriceMap = {};
    for (const livePrice of Object.values(livePrices)) {
      if (canonicalSymbol(livePrice.tick.tradingPair) === tradingPair) {
        result[livePrice.tick.exchangeId as ExchangeId] = livePrice;
      }
    }
    return result;
  }, [livePrices, tradingPair]);

  return { prices, wsStatus };
}
