import { useAnalyticsSummary } from '@/hooks/useAnalytics';

interface StatChipProps {
  label: string;
  value: number | undefined;
  isLoading: boolean;
}

function StatChip({ label, value, isLoading }: StatChipProps) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span className="text-gray-500 text-xs">{label}</span>
      {isLoading ? (
        <span className="inline-block w-6 h-3 bg-gray-700 rounded animate-pulse" />
      ) : (
        <span className="text-white text-sm font-semibold tabular-nums">
          {value ?? '—'}
        </span>
      )}
    </div>
  );
}

/**
 * Compact opportunity count display for the Live Monitor page header.
 *
 * Fetches the analytics summary for 1-hour and 24-hour windows using the existing
 * {@code /api/analytics/summary} endpoint (TanStack Query, 30s poll, 25s stale time).
 * The backend minimum window is 1 hour — these counts reflect CLOSED+EXPIRED
 * opportunities that made it through the full detection→simulation pipeline.
 *
 * Renders a horizontal stat strip: "Last 1h: X · Last 24h: Y opportunities"
 * with skeleton placeholders while fetching.
 */
export function OpportunityCounter() {
  const { data: summary1h,  isLoading: loading1h  } = useAnalyticsSummary(1);
  const { data: summary24h, isLoading: loading24h } = useAnalyticsSummary(24);

  return (
    <div className="flex items-center gap-4 px-4 py-2 rounded-lg border border-surface-border bg-surface">
      <span className="text-gray-500 text-xs uppercase tracking-wider font-medium shrink-0">
        Opportunities
      </span>
      <div className="h-3.5 w-px bg-surface-border" />
      <StatChip label="Last 1h"  value={summary1h?.totalOpportunities}  isLoading={loading1h}  />
      <div className="h-3.5 w-px bg-surface-border" />
      <StatChip label="Last 24h" value={summary24h?.totalOpportunities} isLoading={loading24h} />
    </div>
  );
}
