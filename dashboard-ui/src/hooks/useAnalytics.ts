import { useQuery } from '@tanstack/react-query';
import type {
  AnalyticsSummary,
  HourlyCount,
  ExchangePairStats,
  SpreadBucket,
  DurationBucket,
  CumulativePnlPoint,
  LatencyProfitabilityPoint,
  HeatmapCell,
  FalsePositiveRatePoint,
  LatencyPercentilesResponse,
} from '@/types';

const ANALYTICS_BASE = '/api/analytics';
const REFETCH_INTERVAL_MS = 30_000;
const STALE_TIME_MS = 25_000;

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Analytics fetch failed: ${response.status} ${url}`);
  }
  return response.json() as Promise<T>;
}

/**
 * Fetches the aggregate analytics summary for the given time window.
 *
 * Polls every 30 seconds. Data is considered stale after 25 seconds so TanStack
 * Query triggers a background refetch 5 seconds before the next poll is due,
 * keeping the UI data fresh without visible loading states between refreshes.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useAnalyticsSummary(windowHours = 24) {
  return useQuery<AnalyticsSummary>({
    queryKey: ['analytics', 'summary', windowHours],
    queryFn: () => fetchJson<AnalyticsSummary>(`${ANALYTICS_BASE}/summary?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches per-hour opportunity counts for the given time window.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useHourlyCounts(windowHours = 24) {
  return useQuery<HourlyCount[]>({
    queryKey: ['analytics', 'hourly', windowHours],
    queryFn: () => fetchJson<HourlyCount[]>(`${ANALYTICS_BASE}/hourly?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches opportunity counts grouped by buy/sell exchange route.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useExchangePairStats(windowHours = 24) {
  return useQuery<ExchangePairStats[]>({
    queryKey: ['analytics', 'exchange-pairs', windowHours],
    queryFn: () => fetchJson<ExchangePairStats[]>(`${ANALYTICS_BASE}/by-exchange-pair?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches opportunity counts bucketed by net spread bps for the histogram chart.
 * Six buckets: Negative, 0–5, 5–10, 10–20, 20–50, 50+.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useSpreadDistribution(windowHours = 24) {
  return useQuery<SpreadBucket[]>({
    queryKey: ['analytics', 'spread-distribution', windowHours],
    queryFn: () => fetchJson<SpreadBucket[]>(`${ANALYTICS_BASE}/spread-distribution?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches CLOSED/EXPIRED opportunity counts bucketed by duration in ms.
 * Six buckets: 0–50ms, 50–100ms, 100–200ms, 200–500ms, 500ms–1s, 1s+.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useDurationDistribution(windowHours = 24) {
  return useQuery<DurationBucket[]>({
    queryKey: ['analytics', 'duration-distribution', windowHours],
    queryFn: () => fetchJson<DurationBucket[]>(`${ANALYTICS_BASE}/duration-distribution?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches (latencyMs, netProfit) scatter points from simulation results.
 * Used for the Latency vs Profitability chart — each point is one execution simulation.
 * The frontend computes the linear regression trendline and breakeven annotation from these points.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useLatencyProfitability(windowHours = 24) {
  return useQuery<LatencyProfitabilityPoint[]>({
    queryKey: ['analytics', 'latency-profitability', windowHours],
    queryFn: () => fetchJson<LatencyProfitabilityPoint[]>(`${ANALYTICS_BASE}/latency-profitability?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches heatmap cells grouped by UTC day-of-week × hour-of-day.
 * Default window is 168 hours (7 days) so all weekdays appear in the grid.
 * Empty cells are absent from the response; the frontend renders them as grey.
 *
 * @param windowHours lookback window in hours (default 168 = 7 days)
 */
export function useHeatmapData(windowHours = 168) {
  return useQuery<HeatmapCell[]>({
    queryKey: ['analytics', 'heatmap', windowHours],
    queryFn: () => fetchJson<HeatmapCell[]>(`${ANALYTICS_BASE}/heatmap?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches hourly false positive rates for resolved (CLOSED + EXPIRED) opportunities.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useFalsePositiveRate(windowHours = 24) {
  return useQuery<FalsePositiveRatePoint[]>({
    queryKey: ['analytics', 'false-positive-rate', windowHours],
    queryFn: () => fetchJson<FalsePositiveRatePoint[]>(`${ANALYTICS_BASE}/false-positive-rate?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}

/**
 * Fetches current p50/p95/p99/p999 latency percentiles for all pipeline stages
 * from the HdrHistogram snapshots in the detection engine.
 *
 * Used by the scatter chart to overlay the measured END_TO_END p99 as a reference
 * line showing real pipeline performance alongside simulated execution latency.
 * Returns 0 for all values until the first 10-second snapshot interval completes.
 */
export function useLatencyPercentiles() {
  return useQuery<LatencyPercentilesResponse>({
    queryKey: ['latency', 'percentiles'],
    queryFn: () => fetchJson<LatencyPercentilesResponse>('/api/latency/percentiles'),
    refetchInterval: 30_000,
    staleTime: 25_000,
  });
}

/**
 * Fetches the cumulative P&L time-series for CLOSED opportunities.
 * Each point has a timestamp plus cumulative theoretical and simulated net profit.
 *
 * @param windowHours lookback window in hours (default 24)
 */
export function useCumulativePnl(windowHours = 24) {
  return useQuery<CumulativePnlPoint[]>({
    queryKey: ['analytics', 'cumulative-pnl', windowHours],
    queryFn: () => fetchJson<CumulativePnlPoint[]>(`${ANALYTICS_BASE}/cumulative-pnl?hours=${windowHours}`),
    refetchInterval: REFETCH_INTERVAL_MS,
    staleTime: STALE_TIME_MS,
  });
}
