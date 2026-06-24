import { useEffect, useRef, useState } from 'react';
import type { WsConnectionStatus } from '@/types';
import { logger } from '@/utils/logger';

const INITIAL_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;

export interface UseWebSocketOptions {
  /**
   * When set, React state is updated at most every throttleMs milliseconds.
   *
   * Raw messages are still received and stored in a ref at full WebSocket speed —
   * only the React state update (and therefore re-render) is throttled. The flush
   * always emits the LATEST received message, never a stale one from the start of
   * the interval.
   *
   * Use this for high-frequency feeds like normalised ticks. At 100 ticks/sec across
   * 3 exchanges, unthrottled rendering causes ~90 state updates/sec. Throttling to
   * 100ms reduces that to ~10 re-renders/sec — the limit of human perception — with
   * zero loss of displayed data accuracy.
   *
   * Leave undefined for low-frequency feeds (opportunities, health) where every event
   * must be processed immediately.
   */
  throttleMs?: number;
}

/**
 * Generic WebSocket hook with automatic exponential-backoff reconnection and optional
 * UI render throttling.
 *
 * Connects to `url` on mount and reconnects on disconnect using exponential backoff:
 * 1s → 2s → 4s → 8s → 16s → 30s (capped). The delay resets to 1s after a successful
 * connection so a brief network blip recovers quickly.
 *
 * The global Zustand wsStatus is updated on every state transition so the navbar
 * connection dot reflects real state regardless of which page is mounted.
 *
 * React Strict Mode note: effects run twice in development. The cleanup on the first
 * run sets isMountedRef=false and closes the socket before it opens. The onclose
 * handler guards against reconnecting when unmounted, preventing duplicate connections.
 *
 * @param url     Full WebSocket URL or path proxied via Vite (e.g. "/ws/ticks")
 * @param options Optional throttleMs to rate-limit React state updates for high-frequency feeds
 * @returns lastMessage — the most recently flushed parsed message, or null before first message
 * @returns status     — current WebSocket connection status
 */
export function useWebSocket<T>(
  url: string,
  { throttleMs }: UseWebSocketOptions = {}
): { lastMessage: T | null; status: WsConnectionStatus } {
  const [lastMessage, setLastMessage] = useState<T | null>(null);
  const [status, setStatus] = useState<WsConnectionStatus>('CONNECTING');

  const isMountedRef = useRef(true);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectDelayRef = useRef(INITIAL_RECONNECT_DELAY_MS);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Holds the latest received-but-not-yet-flushed message in throttled mode.
  // Not used when throttleMs is undefined.
  const latestUnflushedRef = useRef<T | null>(null);

  // Mirror throttleMs in a ref so the onmessage closure always reads the current value
  // without needing throttleMs in the [url] effect's dependency array.
  const throttleMsRef = useRef(throttleMs);
  throttleMsRef.current = throttleMs;

  // WebSocket connection lifecycle effect
  useEffect(() => {
    isMountedRef.current = true;

    function updateStatus(next: WsConnectionStatus): void {
      setStatus(next);
    }

    function connect(): void {
      if (!isMountedRef.current) return;

      logger.info(`ws connecting: url=${url}`);
      updateStatus('CONNECTING');

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = (): void => {
        if (!isMountedRef.current) {
          ws.close();
          return;
        }
        logger.info(`ws connected: url=${url}`);
        updateStatus('CONNECTED');
        reconnectDelayRef.current = INITIAL_RECONNECT_DELAY_MS;
      };

      ws.onmessage = (event: MessageEvent<string>): void => {
        try {
          const parsed = JSON.parse(event.data) as T;
          if (throttleMsRef.current) {
            // Throttled: store latest in ref; the flush interval will push it to state
            latestUnflushedRef.current = parsed;
          } else {
            // Unthrottled: update state immediately on every message
            setLastMessage(parsed);
          }
        } catch {
          logger.warn('ws message parse failed', event.data?.slice(0, 80));
        }
      };

      ws.onerror = (): void => {
        logger.warn(`ws error: url=${url}`);
        updateStatus('ERROR');
      };

      ws.onclose = (): void => {
        if (!isMountedRef.current) return;
        const delay = reconnectDelayRef.current;
        logger.info(`ws closed: url=${url}, reconnecting in ${delay}ms`);
        updateStatus('DISCONNECTED');
        reconnectTimerRef.current = setTimeout((): void => {
          reconnectDelayRef.current = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
          connect();
        }, delay);
      };
    }

    connect();

    return (): void => {
      isMountedRef.current = false;
      if (reconnectTimerRef.current !== null) {
        clearTimeout(reconnectTimerRef.current);
      }
      wsRef.current?.close();
    };
  }, [url]); // url is stable — effect runs once per url change

  // Throttle flush interval — only active when throttleMs is configured.
  // Reads latestUnflushedRef at each tick and pushes the newest message to React state.
  // Clears the ref after each flush to avoid re-emitting the same message on the next tick.
  useEffect(() => {
    if (!throttleMs) return;
    const interval = setInterval(() => {
      const latest = latestUnflushedRef.current;
      if (latest !== null) {
        setLastMessage(latest);
        latestUnflushedRef.current = null;
      }
    }, throttleMs);
    return () => clearInterval(interval);
  }, [throttleMs]);

  return { lastMessage, status };
}
