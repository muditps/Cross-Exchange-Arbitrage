import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useOpportunities } from '@/hooks/useOpportunities';
import { useHistoricalOpportunities } from '@/hooks/useHistoricalOpportunities';
import { OpportunityRow } from '@/components/OpportunityRow';
import { OpportunityDetailModal } from '@/components/OpportunityDetailModal';
import type { ClosedOpportunity } from '@/types';

const HISTORICAL_STATUS_STYLES: Record<'CLOSED' | 'EXPIRED', string> = {
  CLOSED:  'bg-gray-500/20 text-gray-400 border border-gray-500/40',
  EXPIRED: 'bg-amber-500/20 text-amber-300 border border-amber-500/40',
};

function fmtBps(raw: string | null | undefined) {
  if (!raw) return '—';
  const n = parseFloat(raw);
  return isNaN(n) ? '—' : `${n.toFixed(2)} bps`;
}

function fmtProfit(raw: string) {
  const n = parseFloat(raw);
  if (isNaN(n)) return '—';
  return `${n >= 0 ? '+' : ''}${n.toFixed(4)}`;
}

function fmtDuration(ms: number) {
  if (!ms) return '—';
  return ms < 1_000 ? `${ms}ms` : `${(ms / 1_000).toFixed(1)}s`;
}

function HistoricalRow({ opp, onClick }: { opp: ClosedOpportunity; onClick: () => void }) {
  const netBps = parseFloat(opp.netSpreadBps);
  return (
    <tr
      className="border-b border-surface-border hover:bg-surface-raised transition-colors cursor-pointer"
      onClick={onClick}
    >
      <td className="px-4 py-3">
        <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold
          ${HISTORICAL_STATUS_STYLES[opp.status as 'CLOSED' | 'EXPIRED'] ?? ''}`}>
          {opp.status}
        </span>
      </td>
      <td className="px-4 py-3 font-mono text-sm text-gray-200">{opp.tradingPair}</td>
      <td className="px-4 py-3 text-sm">
        <span className="text-accent-green font-medium">{opp.buyExchange}</span>
        <span className="text-gray-500 mx-1.5">→</span>
        <span className="text-accent-red font-medium">{opp.sellExchange}</span>
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm">
        <span className={netBps > 0 ? 'text-accent-green' : 'text-accent-red'}>
          {fmtBps(opp.netSpreadBps)}
        </span>
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm text-gray-200">
        {fmtProfit(opp.estimatedProfit)}
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm text-gray-400">
        {fmtDuration(opp.totalDurationMs)}
      </td>
      <td className="px-4 py-3 text-right text-xs text-gray-500 underline decoration-dotted">
        View →
      </td>
    </tr>
  );
}

/**
 * Displays both the real-time OPEN opportunity feed (WebSocket) and the historical
 * CLOSED/EXPIRED table (REST, paginated). Historical filters (pair, exchange direction)
 * are persisted as URL search params so filtered views are bookmarkable and shareable.
 *
 * Clicking any row in the historical table opens {@link OpportunityDetailModal} which
 * fetches full detail + execution simulation for that opportunity.
 */
export function OpportunitiesPage() {
  const { opportunities, wsStatus } = useOpportunities();
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const pair        = searchParams.get('pair')        ?? '';
  const buyExchange = searchParams.get('buyExchange') ?? '';
  const sellExchange = searchParams.get('sellExchange') ?? '';
  const page        = parseInt(searchParams.get('page') ?? '0', 10);

  const { data: historicalPage, isLoading: histLoading } = useHistoricalOpportunities(
    { pair, buyExchange, sellExchange, page }
  );

  function setFilter(key: string, value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) { next.set(key, value); } else { next.delete(key); }
    next.set('page', '0');
    setSearchParams(next, { replace: true });
  }

  function setPage(next: number) {
    const params = new URLSearchParams(searchParams);
    params.set('page', String(next));
    setSearchParams(params, { replace: true });
  }

  const totalPages = historicalPage
    ? Math.ceil(historicalPage.totalCount / historicalPage.size)
    : 0;

  const isConnecting = wsStatus === 'CONNECTING' || wsStatus === 'ERROR';

  return (
    <div className="p-6 space-y-8">
      {/* ── Live feed ─────────────────────────────────────────────────────── */}
      <div>
        <h1 className="text-2xl font-semibold text-white mb-1">Live Opportunities</h1>
        <p className="text-gray-400 text-sm mb-4">
          Real-time feed — all lifecycle events merged by ID, newest first.
        </p>

        {isConnecting && (
          <div className="mb-4 rounded-md bg-amber-500/10 border border-amber-500/30 px-4 py-2 text-sm text-amber-300">
            {wsStatus === 'ERROR'
              ? 'Opportunity feed connection error — retrying…'
              : 'Connecting to opportunity feed…'}
          </div>
        )}

        <div className="rounded-lg border border-surface-border bg-surface overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-surface-border bg-surface-raised">
                {['Status', 'Pair', 'Direction (Buy → Sell)', 'Net Spread', 'Profit (USDT)', 'Duration', 'Detected'].map((h, i) => (
                  <th key={h} className={`px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider ${i >= 3 ? 'text-right' : 'text-left'}`}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {opportunities.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-12 text-center text-gray-500">
                    {wsStatus === 'CONNECTED'
                      ? 'No opportunities detected yet — waiting for market conditions to align.'
                      : 'Waiting for connection…'}
                  </td>
                </tr>
              ) : (
                opportunities.map((opp) => <OpportunityRow key={opp.id} opportunity={opp} />)
              )}
            </tbody>
          </table>
        </div>
        {opportunities.length > 0 && (
          <p className="mt-2 text-xs text-gray-600">
            Showing {opportunities.length} of last 50 opportunities. Rows update in-place as lifecycle events arrive.
          </p>
        )}
      </div>

      {/* ── Historical CLOSED table ───────────────────────────────────────── */}
      <div>
        <h2 className="text-xl font-semibold text-white mb-1">Historical Closed Opportunities</h2>
        <p className="text-gray-400 text-sm mb-4">
          Paginated CLOSED/EXPIRED records from TimescaleDB. Click a row for fee breakdown and simulation result.
          Filters are saved in the URL — bookmark or share the view.
        </p>

        {/* Filter bar */}
        <div className="flex flex-wrap gap-3 mb-4">
          {[
            { key: 'pair',         placeholder: 'Pair (e.g. BTC-USDT)', value: pair },
            { key: 'buyExchange',  placeholder: 'Buy exchange',          value: buyExchange },
            { key: 'sellExchange', placeholder: 'Sell exchange',         value: sellExchange },
          ].map(({ key, placeholder, value }) => (
            <input key={key}
              type="text" value={value} placeholder={placeholder}
              onChange={(e) => setFilter(key, e.target.value)}
              className="rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm text-gray-200
                         placeholder:text-gray-600 focus:border-accent-green focus:outline-none w-44"
            />
          ))}
          {(pair || buyExchange || sellExchange) && (
            <button
              onClick={() => { setSearchParams(new URLSearchParams(), { replace: true }); }}
              className="text-xs text-gray-500 hover:text-gray-300 underline decoration-dotted self-center">
              Clear filters
            </button>
          )}
        </div>

        <div className="rounded-lg border border-surface-border bg-surface overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-surface-border bg-surface-raised">
                {['Status', 'Pair', 'Direction', 'Net Spread', 'Est. Profit', 'Duration', ''].map((h, i) => (
                  <th key={i} className={`px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider ${i >= 3 ? 'text-right' : 'text-left'}`}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {histLoading && (
                <tr>
                  <td colSpan={7} className="px-4 py-12 text-center text-gray-500">Loading…</td>
                </tr>
              )}
              {!histLoading && (!historicalPage || historicalPage.opportunities.length === 0) && (
                <tr>
                  <td colSpan={7} className="px-4 py-12 text-center text-gray-500">
                    No closed opportunities found{pair || buyExchange || sellExchange ? ' matching filters' : ''}.
                  </td>
                </tr>
              )}
              {!histLoading && historicalPage?.opportunities.map((opp) => (
                <HistoricalRow key={opp.id} opp={opp} onClick={() => setSelectedId(opp.id)} />
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between mt-3">
            <span className="text-xs text-gray-500">
              Page {page + 1} of {totalPages} — {historicalPage?.totalCount ?? 0} total
            </span>
            <div className="flex gap-2">
              <button disabled={page === 0} onClick={() => setPage(page - 1)}
                className="px-3 py-1 rounded border border-surface-border text-xs text-gray-300
                           hover:border-accent-green hover:text-white disabled:opacity-30 disabled:cursor-not-allowed">
                ← Previous
              </button>
              <button disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}
                className="px-3 py-1 rounded border border-surface-border text-xs text-gray-300
                           hover:border-accent-green hover:text-white disabled:opacity-30 disabled:cursor-not-allowed">
                Next →
              </button>
            </div>
          </div>
        )}
      </div>

      <OpportunityDetailModal opportunityId={selectedId} onClose={() => setSelectedId(null)} />
    </div>
  );
}
