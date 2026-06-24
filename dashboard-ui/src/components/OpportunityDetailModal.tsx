import { useEffect } from 'react';
import { useOpportunityDetail, useOpportunitySimulation } from '@/hooks/useHistoricalOpportunities';
import type { ClosedOpportunity, SimulationSummary } from '@/types';

interface OpportunityDetailModalProps {
  /** ID of the opportunity to show. Null means modal is closed. */
  opportunityId: string | null;
  onClose: () => void;
}

function fmtPrice(raw: string | null | undefined): string {
  if (!raw) return '—';
  const n = parseFloat(raw);
  return isNaN(n) ? '—' : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

function fmtBps(raw: string | null | undefined): string {
  if (!raw) return '—';
  const n = parseFloat(raw);
  return isNaN(n) ? '—' : `${n.toFixed(4)} bps`;
}

function fmtPct(raw: string | null | undefined): string {
  if (!raw) return '—';
  const n = parseFloat(raw);
  return isNaN(n) ? '—' : `${(n * 100).toFixed(2)}%`;
}

function fmtDuration(ms: number | null | undefined): string {
  if (ms == null) return '—';
  if (ms < 1_000) return `${ms}ms`;
  return `${(ms / 1_000).toFixed(1)}s`;
}

function fmtTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'medium' });
  } catch { return '—'; }
}

interface RowProps { label: string; value: string; highlight?: 'green' | 'red' | 'none'; }

function DetailRow({ label, value, highlight = 'none' }: RowProps) {
  const valueClass = highlight === 'green' ? 'text-accent-green' :
                     highlight === 'red'   ? 'text-accent-red'   : 'text-gray-200';
  return (
    <div className="flex justify-between py-1.5 border-b border-surface-border/40">
      <span className="text-gray-400 text-xs">{label}</span>
      <span className={`font-mono text-xs ${valueClass}`}>{value}</span>
    </div>
  );
}

function FeeBreakdown({ opp }: { opp: ClosedOpportunity }) {
  const buyPrice   = parseFloat(opp.buyPrice);
  const sellPrice  = parseFloat(opp.sellPrice);
  const qty        = parseFloat(opp.quantity);
  const buyFee     = parseFloat(opp.buyFeeRate);
  const sellFee    = parseFloat(opp.sellFeeRate);
  const buyCost    = isNaN(buyPrice * qty * buyFee)   ? null : buyPrice  * qty * buyFee;
  const sellCost   = isNaN(sellPrice * qty * sellFee) ? null : sellPrice * qty * sellFee;
  const totalFee   = buyCost != null && sellCost != null ? buyCost + sellCost : null;
  const grossProfit = !isNaN(buyPrice) && !isNaN(sellPrice) && !isNaN(qty)
    ? (sellPrice - buyPrice) * qty : null;

  return (
    <div>
      <h3 className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-2">Fee Breakdown</h3>
      <DetailRow label={`Buy cost (${opp.buyExchange} ${fmtPct(opp.buyFeeRate)} taker)`}
                 value={buyCost != null ? `-$${buyCost.toFixed(4)}` : '—'} highlight="red" />
      <DetailRow label={`Sell cost (${opp.sellExchange} ${fmtPct(opp.sellFeeRate)} taker)`}
                 value={sellCost != null ? `-$${sellCost.toFixed(4)}` : '—'} highlight="red" />
      <DetailRow label="Total fees"
                 value={totalFee != null ? `-$${totalFee.toFixed(4)}` : '—'} highlight="red" />
      <DetailRow label="Gross profit (before fees)"
                 value={grossProfit != null ? `$${grossProfit.toFixed(4)}` : '—'} />
      <DetailRow label="Net profit estimate"
                 value={fmtPrice(opp.estimatedProfit)}
                 highlight={parseFloat(opp.estimatedProfit) >= 0 ? 'green' : 'red'} />
    </div>
  );
}

function SimulationPanel({ sim }: { sim: SimulationSummary }) {
  return (
    <div>
      <h3 className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-2">Execution Simulation</h3>
      <DetailRow label="Assumed latency"   value={`${sim.simulatedLatencyMs}ms`} />
      <DetailRow label="Slippage"          value={fmtBps(sim.slippageBps)} />
      <DetailRow label="Simulated buy price"  value={fmtPrice(sim.simulatedBuyPrice)} />
      <DetailRow label="Simulated sell price" value={fmtPrice(sim.simulatedSellPrice)} />
      <DetailRow label="Fill probability"  value={fmtPct(sim.fillProbability)} />
      <DetailRow label="Net P&L after slippage"
                 value={fmtPrice(sim.netProfit)}
                 highlight={sim.wasProfitable ? 'green' : 'red'} />
      <div className="mt-2 text-center">
        <span className={`inline-block px-3 py-1 rounded text-xs font-semibold
          ${sim.wasProfitable
            ? 'bg-green-500/20 text-green-300 border border-green-500/40'
            : 'bg-red-500/20 text-red-300 border border-red-500/40'}`}>
          {sim.wasProfitable ? 'PROFITABLE' : 'NOT PROFITABLE'}
        </span>
      </div>
    </div>
  );
}

/**
 * Modal overlay showing full detail for one closed/expired opportunity.
 *
 * Fetches detail + simulation only when an `opportunityId` is provided (modal open).
 * Closes on backdrop click or Escape key. No external modal library — pure Tailwind.
 *
 * @param opportunityId  ID of the opportunity to show; null = closed
 * @param onClose        callback to clear the selected ID
 */
export function OpportunityDetailModal({ opportunityId, onClose }: OpportunityDetailModalProps) {
  const { data: opp, isLoading: oppLoading } = useOpportunityDetail(opportunityId);
  const { data: sim, isLoading: simLoading } = useOpportunitySimulation(opportunityId);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  if (!opportunityId) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="w-full max-w-lg mx-4 rounded-xl border border-surface-border bg-surface shadow-2xl max-h-[90vh] flex flex-col">

        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-surface-border shrink-0">
          <div>
            <h2 className="text-white font-semibold text-sm">Opportunity Detail</h2>
            <p className="text-gray-500 text-xs font-mono mt-0.5 truncate">{opportunityId}</p>
          </div>
          <button onClick={onClose}
            className="text-gray-400 hover:text-white text-xl leading-none ml-4"
            aria-label="Close">×</button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto p-5 space-y-5">
          {oppLoading && (
            <div className="space-y-2 animate-pulse">
              {[...Array(6)].map((_, i) => (
                <div key={i} className="h-4 bg-gray-700 rounded" />
              ))}
            </div>
          )}

          {opp && (
            <>
              {/* Trade summary */}
              <div>
                <h3 className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-2">Trade</h3>
                <DetailRow label="Pair"     value={opp.tradingPair} />
                <DetailRow label="Direction" value={`${opp.buyExchange} → ${opp.sellExchange}`} />
                <DetailRow label="Buy price"  value={fmtPrice(opp.buyPrice)} />
                <DetailRow label="Sell price" value={fmtPrice(opp.sellPrice)} />
                <DetailRow label="Quantity"   value={opp.quantity} />
              </div>

              {/* Spread */}
              <div>
                <h3 className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-2">Spread</h3>
                <DetailRow label="Raw spread"     value={fmtBps(opp.rawSpreadBps)} />
                <DetailRow label="Net spread"
                           value={fmtBps(opp.netSpreadBps)}
                           highlight={parseFloat(opp.netSpreadBps) >= 0 ? 'green' : 'red'} />
                <DetailRow label="Peak net spread"    value={fmtBps(opp.peakNetSpreadBps)} />
                <DetailRow label="Average net spread" value={fmtBps(opp.averageNetSpreadBps)} />
              </div>

              {/* Timeline */}
              <div>
                <h3 className="text-xs font-semibold text-gray-300 uppercase tracking-wider mb-2">Timeline</h3>
                <DetailRow label="Detected"  value={fmtTime(opp.detectionTimestamp)} />
                <DetailRow label="Closed"    value={fmtTime(opp.closedTimestamp)} />
                <DetailRow label="Duration"  value={fmtDuration(opp.totalDurationMs)} />
                <DetailRow label="Status"    value={opp.status} />
              </div>

              <FeeBreakdown opp={opp} />

              {/* Simulation */}
              {simLoading && (
                <div className="animate-pulse h-4 bg-gray-700 rounded w-32" />
              )}
              {!simLoading && sim && <SimulationPanel sim={sim} />}
              {!simLoading && sim === null && (
                <p className="text-xs text-gray-500 italic">
                  No simulation result — opportunity may not have been processed by the execution simulator.
                </p>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
