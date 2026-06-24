import { memo, useEffect, useState } from 'react';
import type { ExchangeId } from '@/types';
import type { LivePriceMap } from '@/hooks/useLivePrices';

const STALE_THRESHOLD_MS = 500;
// 500ms is sufficient for the Age column — human-readable staleness doesn't need sub-200ms
// precision. Was 200ms, causing unnecessary re-renders at 5/sec for display-only data.
const DISPLAY_REFRESH_INTERVAL_MS = 500;

const EXCHANGE_LABELS: Record<ExchangeId, string> = {
  BINANCE: 'Binance',
  BYBIT: 'Bybit',
  KUCOIN: 'KuCoin',
};

const ALL_EXCHANGES: ExchangeId[] = ['BINANCE', 'BYBIT', 'KUCOIN'];

/** Formats a BigDecimal string for display with up to 8 decimal places. */
function fmtPrice(raw: string): string {
  const n = parseFloat(raw);
  if (isNaN(n)) return '—';
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

/** Computes ask − bid spread. Returns '—' if either value is unparseable. */
function fmtSpread(bid: string, ask: string): string {
  const bidN = parseFloat(bid);
  const askN = parseFloat(ask);
  if (isNaN(bidN) || isNaN(askN)) return '—';
  return (askN - bidN).toFixed(4);
}

/** Formats milliseconds as a human-readable age string. */
function fmtAge(ageMs: number): string {
  if (ageMs < 1000) return `${ageMs}ms`;
  return `${(ageMs / 1000).toFixed(1)}s`;
}

interface PriceTableProps {
  prices: LivePriceMap;
}

/**
 * Renders a real-time best bid/ask price table with one row per exchange.
 *
 * Stale detection: a row is marked stale (grey indicator, red age text) when
 * the last received tick is older than STALE_THRESHOLD_MS (500ms — matches the
 * backend staleness threshold in DetectionProperties). The component re-renders
 * every DISPLAY_REFRESH_INTERVAL_MS to keep the age column ticking accurately.
 *
 * Spread is displayed for information only. It is computed as ask − bid using
 * parseFloat — display precision is sufficient here. Financial calculations on
 * the backend always use BigDecimal.
 */
export const PriceTable = memo(function PriceTable({ prices }: PriceTableProps) {
  const [now, setNow] = useState<number>(Date.now);

  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), DISPLAY_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-gray-500 text-xs uppercase tracking-wider border-b border-surface-border">
          <th className="text-left py-2 pr-6 font-normal">Exchange</th>
          <th className="text-right py-2 px-4 font-normal">Best Bid</th>
          <th className="text-right py-2 px-4 font-normal">Best Ask</th>
          <th className="text-right py-2 px-4 font-normal">Spread</th>
          <th className="text-right py-2 pl-4 font-normal">Age</th>
        </tr>
      </thead>
      <tbody>
        {ALL_EXCHANGES.map((exchangeId) => {
          const entry = prices[exchangeId];
          const ageMs = entry ? Math.max(0, now - entry.receivedAt) : null;
          const isStale = ageMs === null || ageMs > STALE_THRESHOLD_MS;

          return (
            <tr
              key={exchangeId}
              className={`border-b border-surface-border/50 transition-colors duration-300 ${
                isStale ? 'opacity-40' : 'opacity-100'
              }`}
            >
              {/* Exchange name + live/stale indicator dot */}
              <td className="py-3 pr-6">
                <div className="flex items-center gap-2">
                  <span
                    className={`w-2 h-2 rounded-full shrink-0 transition-colors ${
                      isStale ? 'bg-gray-600' : 'bg-accent-green'
                    }`}
                  />
                  <span className="font-medium text-gray-200">{EXCHANGE_LABELS[exchangeId]}</span>
                </div>
              </td>

              {/* Best Bid */}
              <td className="text-right py-3 px-4 font-mono text-gray-200">
                {entry ? fmtPrice(entry.tick.bestBidPrice) : '—'}
              </td>

              {/* Best Ask */}
              <td className="text-right py-3 px-4 font-mono text-gray-200">
                {entry ? fmtPrice(entry.tick.bestAskPrice) : '—'}
              </td>

              {/* Spread (ask − bid) */}
              <td className="text-right py-3 px-4 font-mono text-gray-500 text-xs">
                {entry ? fmtSpread(entry.tick.bestBidPrice, entry.tick.bestAskPrice) : '—'}
              </td>

              {/* Tick age — red when stale */}
              <td
                className={`text-right py-3 pl-4 text-xs font-mono ${
                  isStale ? 'text-accent-red' : 'text-gray-500'
                }`}
              >
                {ageMs !== null ? fmtAge(ageMs) : 'no data'}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
});
