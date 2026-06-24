import { useEffect, useRef, type ReactNode } from 'react';
import type { ArbitrageOpportunity, NormalisedTick, WsConnectionStatus } from '@/types';
import { canonicalSymbol } from '@/types';
import { useWebSocket } from '@/hooks/useWebSocket';
import { useArbitrageStore, livePriceKey, type LivePrice } from '@/stores/arbitrageStore';

/**
 * Derives a single connection status from two independent WebSocket statuses.
 *
 * Priority order: CONNECTED > CONNECTING > ERROR > DISCONNECTED.
 * If either feed is CONNECTED, the global indicator shows CONNECTED —
 * the user has live data from at least one source.
 */
function deriveGlobalStatus(
  s1: WsConnectionStatus,
  s2: WsConnectionStatus,
): WsConnectionStatus {
  if (s1 === 'CONNECTED' || s2 === 'CONNECTED') return 'CONNECTED';
  if (s1 === 'CONNECTING' || s2 === 'CONNECTING') return 'CONNECTING';
  if (s1 === 'ERROR' || s2 === 'ERROR') return 'ERROR';
  return 'DISCONNECTED';
}

/**
 * Persistent WebSocket provider mounted once at App level, above the router.
 *
 * Tick buffering strategy: every incoming tick is written into a per-key ref map
 * ({@link priceMapRef}) keyed by "EXCHANGE:PAIR" (e.g. "BINANCE:BNB-USDT"). A
 * 100ms interval flushes the entire map to the Zustand store.
 *
 * This ensures BNB-USDT and ETH-USDT ticks are never overwritten by the higher-
 * frequency BTC-USDT stream before the flush fires — each exchange+pair slot is
 * independent and only updated by its own incoming messages.
 *
 * Prior approach used useWebSocket's single-slot throttle (throttleMs: 100), which
 * stored only the most recent tick in the 100ms window. Under high BTC-USDT
 * throughput, BNB-USDT ticks were dropped before reaching the store.
 */
export function WebSocketProvider({ children }: { children: ReactNode }) {
  const { lastMessage: lastTick, status: ticksStatus } =
    useWebSocket<NormalisedTick>('/ws/ticks'); // no throttleMs — we buffer per-key in priceMapRef
  const { lastMessage: lastOpportunity, status: opportunitiesStatus } =
    useWebSocket<ArbitrageOpportunity>('/ws/opportunities');

  /** Per-key tick accumulator. Mutated on every incoming tick; never causes re-renders itself. */
  const priceMapRef = useRef<Record<string, LivePrice>>({});

  const setLivePrices = useArbitrageStore((s) => s.setLivePrices);
  const setLastOpportunity = useArbitrageStore((s) => s.setLastOpportunity);
  const setWsStatus = useArbitrageStore((s) => s.setWsStatus);

  // Write every tick into its own slot — no tick is overwritten by a different pair/exchange
  useEffect(() => {
    if (lastTick === null) return;
    const key = livePriceKey(lastTick.exchangeId, canonicalSymbol(lastTick.tradingPair));
    priceMapRef.current[key] = { tick: lastTick, receivedAt: Date.now() };
  }, [lastTick]);

  // Flush accumulated map to store at 100ms — rate-limits React renders without losing any pair
  useEffect(() => {
    const interval = setInterval(() => {
      const snapshot = { ...priceMapRef.current };
      if (Object.keys(snapshot).length > 0) {
        setLivePrices(snapshot);
      }
    }, 100);
    return () => clearInterval(interval);
  }, [setLivePrices]);

  useEffect(() => {
    if (lastOpportunity !== null) setLastOpportunity(lastOpportunity);
  }, [lastOpportunity, setLastOpportunity]);

  useEffect(() => {
    setWsStatus(deriveGlobalStatus(ticksStatus, opportunitiesStatus));
  }, [ticksStatus, opportunitiesStatus, setWsStatus]);

  return <>{children}</>;
}
