import { useArbitrageStore } from '@/stores/arbitrageStore';
import { useLivePrices } from '@/hooks/useLivePrices';
import { PriceTable } from '@/components/PriceTable';
import { CrossExchangeSpreadMatrix } from '@/components/CrossExchangeSpreadMatrix';
import { OpportunityCounter } from '@/components/OpportunityCounter';

const SUPPORTED_PAIRS = ['BTC-USDT', 'ETH-USDT', 'BNB-USDT'] as const;

/**
 * Live Monitor page — real-time best bid/ask prices and cross-exchange spread analysis.
 *
 * Three sections:
 * 1. {@link PriceTable} — per-exchange best bid/ask, spread, staleness indicator.
 * 2. {@link CrossExchangeSpreadMatrix} — net spread matrix for all 6 exchange directions,
 *    computed client-side from live prices (no extra API call). Positive cells are
 *    potential arbitrage opportunities after fee deduction.
 * 3. {@link OpportunityCounter} — 1h and 24h opportunity counts from the analytics REST API.
 *
 * The pair selector is persisted in the Zustand {@link useArbitrageStore}. The WebSocket
 * connection is maintained by {@link useLivePrices}, which filters ticks client-side so
 * only the selected pair contributes to the price map.
 *
 * The connection banner is shown only during CONNECTING / ERROR states.
 * DISCONNECTED is a terminal state the user cannot act on from here;
 * CONNECTED is the nominal state — no banner needed in either case.
 */
export function LiveMonitorPage() {
  const selectedPair = useArbitrageStore((s) => s.selectedPair);
  const setSelectedPair = useArbitrageStore((s) => s.setSelectedPair);
  const { prices, wsStatus } = useLivePrices(selectedPair);

  const showBanner = wsStatus === 'CONNECTING' || wsStatus === 'ERROR';
  const bannerText =
    wsStatus === 'CONNECTING' ? 'Connecting to live feed...' : 'Feed error — reconnecting...';

  return (
    <div className="p-6 max-w-5xl space-y-6">
      {/* ── Page header ─────────────────────────────────────────────────── */}
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-white mb-1">Live Monitor</h1>
          <p className="text-gray-400 text-sm">
            Real-time best bid/ask prices across Binance, Bybit, and KuCoin.
          </p>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          {/* Opportunity counts */}
          <OpportunityCounter />

          {/* Pair selector */}
          <select
            value={selectedPair}
            onChange={(e) => setSelectedPair(e.target.value)}
            className="bg-surface-raised border border-surface-border text-gray-200 text-sm rounded px-3 py-1.5 focus:outline-none focus:ring-1 focus:ring-accent-blue cursor-pointer"
          >
            {SUPPORTED_PAIRS.map((pair) => (
              <option key={pair} value={pair}>
                {pair}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Connection status banner — only during CONNECTING or ERROR */}
      {showBanner && (
        <div className="rounded border border-accent-yellow/30 bg-accent-yellow/10 px-4 py-2 text-accent-yellow text-sm">
          {bannerText}
        </div>
      )}

      {/* ── Live price table ─────────────────────────────────────────────── */}
      <div>
        <div className="rounded-lg border border-surface-border bg-surface-raised px-4 py-1">
          <PriceTable prices={prices} />
        </div>
        <p className="mt-2 text-xs text-gray-600">
          Row fades when last tick is older than 500ms (backend staleness threshold).
        </p>
      </div>

      {/* ── Cross-exchange spread matrix ─────────────────────────────────── */}
      <div className="rounded-lg border border-surface-border bg-surface p-4">
        <CrossExchangeSpreadMatrix prices={prices} tradingPair={selectedPair} />
      </div>
    </div>
  );
}
