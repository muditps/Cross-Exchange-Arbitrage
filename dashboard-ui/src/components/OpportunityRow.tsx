import type { ArbitrageOpportunity, OpportunityStatus } from '@/types';
import { canonicalSymbol } from '@/types';

interface OpportunityRowProps {
  opportunity: ArbitrageOpportunity;
}

const STATUS_STYLES: Record<OpportunityStatus, string> = {
  DETECTED: 'bg-blue-500/20 text-blue-300 border border-blue-500/40',
  OPEN:     'bg-green-500/20 text-green-300 border border-green-500/40',
  CLOSED:   'bg-gray-500/20 text-gray-400 border border-gray-500/40',
  EXPIRED:  'bg-amber-500/20 text-amber-300 border border-amber-500/40',
};

function formatDuration(ms: number): string {
  if (ms === 0 || ms == null) return '—';
  if (ms < 1_000) return `${ms}ms`;
  return `${(ms / 1_000).toFixed(1)}s`;
}

function formatProfit(raw: string): string {
  const n = parseFloat(raw);
  if (isNaN(n)) return '—';
  const sign = n >= 0 ? '+' : '';
  return `${sign}${n.toFixed(4)}`;
}

function formatTime(isoString: string): string {
  try {
    return new Date(isoString).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  } catch {
    return '—';
  }
}

/**
 * Renders one row in the OpportunitiesPage table.
 *
 * Displays: status badge, trading pair, buy→sell exchange direction,
 * net spread in basis points, theoretical profit (USDT), total duration,
 * and detection time. All numeric display uses parseFloat — financial
 * calculations remain server-side in BigDecimal.
 *
 * @param opportunity The ArbitrageOpportunity to render
 */
export function OpportunityRow({ opportunity: opp }: OpportunityRowProps) {
  const netSpreadBps = parseFloat(opp.netSpreadBps);

  return (
    <tr className="border-b border-surface-border hover:bg-surface-raised transition-colors">
      <td className="px-4 py-3">
        <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${STATUS_STYLES[opp.status]}`}>
          {opp.status}
        </span>
      </td>
      <td className="px-4 py-3 font-mono text-sm text-gray-200">
        {canonicalSymbol(opp.tradingPair)}
      </td>
      <td className="px-4 py-3 text-sm">
        <span className="text-accent-green font-medium">{opp.buyExchange}</span>
        <span className="text-gray-500 mx-1.5">→</span>
        <span className="text-accent-red font-medium">{opp.sellExchange}</span>
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm">
        <span className={netSpreadBps > 0 ? 'text-accent-green' : 'text-accent-red'}>
          {isNaN(netSpreadBps) ? '—' : `${netSpreadBps.toFixed(2)} bps`}
        </span>
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm text-gray-200">
        {formatProfit(opp.theoreticalProfit)}
      </td>
      <td className="px-4 py-3 text-right font-mono text-sm text-gray-400">
        {formatDuration(opp.totalDurationMs)}
      </td>
      <td className="px-4 py-3 text-right text-xs text-gray-500">
        {formatTime(opp.detectionTimestamp)}
      </td>
    </tr>
  );
}
