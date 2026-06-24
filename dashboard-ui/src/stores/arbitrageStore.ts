import { create } from 'zustand';
import type { ArbitrageOpportunity, ExchangeId, NormalisedTick, WsConnectionStatus } from '../types';

/** A NormalisedTick paired with the browser wall-clock time it was received. */
export interface LivePrice {
  tick: NormalisedTick;
  /** Date.now() at the moment this tick arrived in the browser. Used for stale detection. */
  receivedAt: number;
}

/**
 * Client-side UI state managed by Zustand.
 *
 * Server state (tick data, opportunities, history) lives in TanStack Query caches.
 * What lives here:
 * - selectedPair: which trading pair the user is viewing across all pages
 * - wsStatus: global WebSocket connection state derived from both persistent feeds
 * - livePrices: per-exchange, per-pair prices keyed by "EXCHANGE:PAIR" (e.g. "BINANCE:BTC-USDT")
 * - lastOpportunity: latest opportunity message written by WebSocketProvider
 * - isSidebarOpen: responsive layout state
 */
interface ArbitrageState {
  selectedPair: string;
  wsStatus: WsConnectionStatus;
  isSidebarOpen: boolean;
  livePrices: Record<string, LivePrice>;
  lastOpportunity: ArbitrageOpportunity | null;
  setSelectedPair: (pair: string) => void;
  setWsStatus: (status: WsConnectionStatus) => void;
  toggleSidebar: () => void;
  setLivePrices: (prices: Record<string, LivePrice>) => void;
  setLastOpportunity: (opp: ArbitrageOpportunity) => void;
}

export const useArbitrageStore = create<ArbitrageState>((set) => ({
  selectedPair: 'BTC-USDT',
  wsStatus: 'DISCONNECTED',
  isSidebarOpen: true,
  livePrices: {},
  lastOpportunity: null,

  setSelectedPair: (pair) => set({ selectedPair: pair }),
  setWsStatus: (status) => set({ wsStatus: status }),
  toggleSidebar: () => set((state) => ({ isSidebarOpen: !state.isSidebarOpen })),
  setLivePrices: (prices) => set({ livePrices: prices }),
  setLastOpportunity: (opp) => set({ lastOpportunity: opp }),
}));

/** Builds the map key for a given exchange + trading pair. */
export function livePriceKey(exchangeId: ExchangeId, pair: string): string {
  return `${exchangeId}:${pair}`;
}
