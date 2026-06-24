import { useEffect, useRef, useState } from 'react';
import type { ArbitrageOpportunity, WsConnectionStatus } from '@/types';
import { useArbitrageStore } from '@/stores/arbitrageStore';

const MAX_OPPORTUNITIES = 50;

/**
 * Maintains a deduplicated, time-sorted list of the most recent arbitrage opportunities,
 * reading from the Zustand store written by {@link WebSocketProvider}.
 *
 * The WS connection is NOT managed here — it lives in WebSocketProvider at App level
 * so it persists across page navigation. This hook is a pure read from store state.
 *
 * Deduplication strategy: the detection engine emits DETECTED → OPEN → CLOSED for the
 * same opportunity id. This hook merges updates by id — one row per opportunity that
 * transitions through status states live. Updates preserve row position so rows don't jump.
 *
 * Ring buffer: capped at MAX_OPPORTUNITIES entries. When full, the oldest entry is evicted.
 * Sorted by detectionTimestamp descending — newest at the top.
 *
 * @returns opportunities — deduplicated list, newest first, max 50 entries
 * @returns wsStatus — global WebSocket connection status
 */
export function useOpportunities(): {
  opportunities: ArbitrageOpportunity[];
  wsStatus: WsConnectionStatus;
} {
  const lastOpportunity = useArbitrageStore((s) => s.lastOpportunity);
  const wsStatus = useArbitrageStore((s) => s.wsStatus);

  const [opportunities, setOpportunities] = useState<ArbitrageOpportunity[]>([]);
  const opportunitiesRef = useRef<ArbitrageOpportunity[]>([]);

  useEffect(() => {
    if (lastOpportunity === null) return;

    const incoming = lastOpportunity;
    const current = opportunitiesRef.current;
    const existingIndex = current.findIndex((opp) => opp.id === incoming.id);

    let next: ArbitrageOpportunity[];

    if (existingIndex !== -1) {
      // Update existing entry in-place — preserve its position so rows don't jump
      next = [...current];
      next[existingIndex] = incoming;
    } else {
      // New opportunity: prepend then trim to ring buffer cap
      const appended = [incoming, ...current];
      next = appended.length > MAX_OPPORTUNITIES
        ? appended.slice(0, MAX_OPPORTUNITIES)
        : appended;
    }

    opportunitiesRef.current = next;
    setOpportunities(next);
  }, [lastOpportunity]);

  return { opportunities, wsStatus };
}
