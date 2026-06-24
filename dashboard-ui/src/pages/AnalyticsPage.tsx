import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LineChart,
  Line,
  Legend,
  ComposedChart,
  Scatter,
  ReferenceLine,
} from 'recharts';
import {
  useAnalyticsSummary,
  useHourlyCounts,
  useExchangePairStats,
  useSpreadDistribution,
  useDurationDistribution,
  useCumulativePnl,
  useLatencyProfitability,
  useHeatmapData,
  useFalsePositiveRate,
  useLatencyPercentiles,
} from '@/hooks/useAnalytics';
import type { ExchangePairStats } from '@/types';
import { computeTrendLine, computeBreakevenLatencyMs, type TrendPoint } from '@/utils/analytics';

const WINDOW_HOURS = 24;
const HEATMAP_WINDOW_HOURS = 168; // 7 days — needs full week for day-of-week axis to be meaningful

// Tailwind custom colours expressed as hex for Recharts (can't use CSS classes in SVG fill)
const CHART_GREEN = '#34d399';
const CHART_BLUE  = '#60a5fa';
const CHART_RED   = '#f87171';
const CHART_MUTED = '#374151';

const DAYS_OF_WEEK = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const HOURS_OF_DAY = Array.from({ length: 24 }, (_, i) => i);

function formatHourLabel(isoString: string): string {
  try {
    return new Date(isoString).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function pairLabel(stat: ExchangePairStats): string {
  return `${stat.buyExchange.slice(0, 3)}→${stat.sellExchange.slice(0, 3)}`;
}

interface StatCardProps {
  label: string;
  value: string | number;
  sub?: string;
}

function StatCard({ label, value, sub }: StatCardProps) {
  return (
    <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-2xl font-mono font-semibold text-white">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-0.5">{sub}</p>}
    </div>
  );
}

/**
 * Displays historical analytics for the arbitrage detection platform.
 *
 * Data is fetched via TanStack Query (REST, 30s poll) — not WebSocket.
 * Historical analytics change slowly; polling is more appropriate than
 * a persistent connection.
 *
 * Charts:
 * 1. Summary stat cards: total opportunities, avg spread, simulation win rate
 * 2. Hourly bar chart: opportunities detected per hour (last 24h)
 * 3. Exchange pair bar chart: which buy→sell routes generate the most opportunities
 * 4. Spread distribution histogram: net spread bps buckets (red=negative, green=positive)
 * 5. Duration distribution histogram: how long opportunities live before closing
 * 6. Cumulative P&L line chart: theoretical vs simulated profit over time
 */
export function AnalyticsPage() {
  const { data: summary, isLoading: summaryLoading, isError: summaryError } = useAnalyticsSummary(WINDOW_HOURS);
  const { data: hourly = [], isLoading: hourlyLoading } = useHourlyCounts(WINDOW_HOURS);
  const { data: pairs = [], isLoading: pairsLoading } = useExchangePairStats(WINDOW_HOURS);
  const { data: spreadDist = [], isLoading: spreadLoading } = useSpreadDistribution(WINDOW_HOURS);
  const { data: durationDist = [], isLoading: durationLoading } = useDurationDistribution(WINDOW_HOURS);
  const { data: cumulativePnl = [], isLoading: pnlLoading } = useCumulativePnl(WINDOW_HOURS);
  const { data: latencyProfitRaw = [], isLoading: scatterLoading } = useLatencyProfitability(WINDOW_HOURS);
  const { data: heatmapRaw = [], isLoading: heatmapLoading } = useHeatmapData(HEATMAP_WINDOW_HOURS);
  const { data: fprData = [], isLoading: fprLoading } = useFalsePositiveRate(WINDOW_HOURS);
  const { data: latencyPercentiles } = useLatencyPercentiles();

  const endToEndP99Ms = latencyPercentiles?.stages['END_TO_END']?.p99Ms ?? null;
  const hasValidP99 = endToEndP99Ms !== null && endToEndP99Ms > 0;

  // Scatter data — convert string netProfit to number for Recharts axes
  const scatterPoints: TrendPoint[] = latencyProfitRaw.map(p => ({
    x: p.latencyMs,
    y: parseFloat(p.netProfit),
  }));
  const trendLine = computeTrendLine(scatterPoints);
  const breakevenMs = computeBreakevenLatencyMs(trendLine);

  // Heatmap — build lookup map keyed by "dow-hour" for O(1) cell access
  const heatmapMap = new Map(heatmapRaw.map(c => [`${c.dayOfWeek}-${c.hourOfDay}`, c]));
  const heatmapMax = Math.max(...heatmapRaw.map(c => c.avgNetSpreadBps), 1);

  const pairsWithLabel = pairs.map((p) => ({ ...p, label: pairLabel(p) }));

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-white mb-1">Analytics</h1>
        <p className="text-gray-400 text-sm">
          Historical spread distributions, opportunity frequency, and simulated P&amp;L — last {WINDOW_HOURS}h.
        </p>
      </div>

      {/* ── Summary cards ── */}
      {summaryError && (
        <div className="rounded-md bg-red-500/10 border border-red-500/30 px-4 py-2 text-sm text-red-300">
          Failed to load analytics — is the backend running?
        </div>
      )}

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatCard
          label="Opportunities (24h)"
          value={summaryLoading ? '…' : (summary?.totalOpportunities ?? 0)}
          sub={`${summary?.activeOpportunities ?? 0} active now`}
        />
        <StatCard
          label="Closed"
          value={summaryLoading ? '…' : (summary?.closedOpportunities ?? 0)}
          sub={`${summary?.expiredOpportunities ?? 0} expired`}
        />
        <StatCard
          label="Avg Net Spread"
          value={summaryLoading ? '…' : `${summary?.avgNetSpreadBps?.toFixed(1) ?? '0.0'} bps`}
          sub={`peak ${summary?.maxNetSpreadBps?.toFixed(1) ?? '0.0'} bps`}
        />
        <StatCard
          label="Sim Win Rate"
          value={summaryLoading ? '…' : `${summary?.winRatePct?.toFixed(1) ?? '0.0'}%`}
          sub={`${summary?.profitableSimulations ?? 0} / ${summary?.totalSimulations ?? 0} sims`}
        />
      </div>

      {/* ── Hourly bar chart ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Opportunities per Hour (last {WINDOW_HOURS}h)</h2>
        {hourlyLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : hourly.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No data in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={hourly} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} vertical={false} />
              <XAxis
                dataKey="hour"
                tickFormatter={formatHourLabel}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                itemStyle={{ color: CHART_GREEN }}
                labelFormatter={(label) => formatHourLabel(String(label))}
                formatter={(value) => [value ?? 0, 'Opportunities']}
              />
              <Bar dataKey="count" fill={CHART_GREEN} radius={[3, 3, 0, 0]} maxBarSize={40} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Exchange pair breakdown ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">By Exchange Route (last {WINDOW_HOURS}h)</h2>
        {pairsLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : pairsWithLabel.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No data in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={Math.max(120, pairsWithLabel.length * 44)}>
            <BarChart
              data={pairsWithLabel}
              layout="vertical"
              margin={{ top: 4, right: 40, left: 8, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} horizontal={false} />
              <XAxis
                type="number"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />
              <YAxis
                type="category"
                dataKey="label"
                tick={{ fill: '#d1d5db', fontSize: 12 }}
                axisLine={false}
                tickLine={false}
                width={56}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                itemStyle={{ color: CHART_BLUE }}
                formatter={(value, _name, entry) => {
                  const stat = entry.payload as ExchangePairStats & { label: string };
                  return [`${value ?? 0} opps · ${stat.avgNetSpreadBps.toFixed(1)} bps avg`, stat.label];
                }}
              />
              <Bar dataKey="count" radius={[0, 3, 3, 0]} maxBarSize={28}>
                {pairsWithLabel.map((entry) => (
                  <Cell key={entry.label} fill={CHART_BLUE} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Spread distribution histogram ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">Net Spread Distribution (last {WINDOW_HOURS}h)</h2>
        <p className="text-xs text-gray-500 mb-4">
          Red = below-cost spreads (fees &gt; gross spread). Green = potentially profitable.
        </p>
        {spreadLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : spreadDist.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No data in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={spreadDist} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} vertical={false} />
              <XAxis
                dataKey="bucketLabel"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                formatter={(value) => [value ?? 0, 'Opportunities']}
              />
              <Bar dataKey="count" radius={[3, 3, 0, 0]} maxBarSize={48}>
                {spreadDist.map((entry) => (
                  <Cell
                    key={entry.bucketLabel}
                    fill={entry.bucketLabel === 'Negative' ? CHART_RED : CHART_GREEN}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Duration distribution histogram ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">Opportunity Duration Distribution (last {WINDOW_HOURS}h)</h2>
        <p className="text-xs text-gray-500 mb-4">
          How long CLOSED/EXPIRED opportunities persisted before the price discrepancy resolved.
        </p>
        {durationLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : durationDist.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No data in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={durationDist} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} vertical={false} />
              <XAxis
                dataKey="bucketLabel"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                itemStyle={{ color: CHART_BLUE }}
                formatter={(value) => [value ?? 0, 'Opportunities']}
              />
              <Bar dataKey="count" fill={CHART_BLUE} radius={[3, 3, 0, 0]} maxBarSize={48} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Latency vs Profitability scatter ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">
          Latency vs Profitability (last {WINDOW_HOURS}h)
        </h2>
        <p className="text-xs text-gray-500 mb-4">
          Each point is one execution simulation. Trendline (OLS linear regression) shows the relationship
          between speed and profit — the fundamental HFT trade-off.
          {breakevenMs !== null && (
            <span className="ml-1 text-amber-400 font-medium">
              Breakeven latency: ~{breakevenMs}ms — above this threshold the simulated trade loses money.
            </span>
          )}
        </p>
        {scatterLoading ? (
          <div className="h-56 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : scatterPoints.length === 0 ? (
          <div className="h-56 flex items-center justify-center text-gray-500 text-sm">
            No simulation results in window — run the execution simulator to populate this chart
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} />
              <XAxis
                dataKey="x"
                type="number"
                name="Latency (ms)"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                label={{ value: 'Latency (ms)', position: 'insideBottom', offset: -2, fill: '#6b7280', fontSize: 11 }}
              />
              <YAxis
                dataKey="y"
                type="number"
                name="Net Profit"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => v.toFixed(4)}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                formatter={(value: unknown, name: unknown) => [
                  typeof value === 'number' ? value.toFixed(6) : String(value),
                  name === 'y' ? 'Net Profit' : 'Latency (ms)',
                ]}
              />
              <ReferenceLine y={0} stroke={CHART_MUTED} strokeWidth={1.5} strokeDasharray="4 2" />
              {hasValidP99 && (
                <ReferenceLine
                  x={endToEndP99Ms}
                  stroke="#f59e0b"
                  strokeWidth={1.5}
                  strokeDasharray="5 3"
                  label={{ value: `Pipeline p99 (${endToEndP99Ms.toFixed(0)}ms)`, position: 'insideTopLeft', fill: '#f59e0b', fontSize: 11 }}
                />
              )}
              <Scatter data={scatterPoints} fill={CHART_BLUE} opacity={0.55} name="Simulation" />
              {trendLine && (
                <Line
                  data={trendLine}
                  dataKey="y"
                  type="linear"
                  stroke={CHART_RED}
                  strokeWidth={2}
                  strokeDasharray="6 3"
                  dot={false}
                  legendType="none"
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Cumulative P&L line chart ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">Cumulative Simulated P&amp;L (last {WINDOW_HOURS}h)</h2>
        <p className="text-xs text-gray-500 mb-4">
          Theoretical = estimated profit before slippage. Simulated = execution-simulator net profit after latency and slippage modelling.
        </p>
        {pnlLoading ? (
          <div className="h-56 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : cumulativePnl.length === 0 ? (
          <div className="h-56 flex items-center justify-center text-gray-500 text-sm">No closed opportunities in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <LineChart
              data={cumulativePnl.map((p) => ({
                ...p,
                grossNum: parseFloat(p.cumulativeGrossProfit),
                simNum:   parseFloat(p.cumulativeSimProfit),
              }))}
              margin={{ top: 4, right: 8, left: 4, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} />
              <XAxis
                dataKey="ts"
                tickFormatter={(v) => formatHourLabel(String(v))}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => v.toFixed(2)}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                labelFormatter={(label) => formatHourLabel(String(label))}
                formatter={(value: unknown, name: unknown) => [
                  typeof value === 'number' ? value.toFixed(6) : String(value),
                  name === 'grossNum' ? 'Theoretical' : 'Simulated',
                ]}
              />
              <Legend
                formatter={(value) => (
                  <span style={{ color: '#9ca3af', fontSize: 12 }}>
                    {value === 'grossNum' ? 'Theoretical profit' : 'Simulated net profit'}
                  </span>
                )}
              />
              <Line
                type="monotone"
                dataKey="grossNum"
                stroke={CHART_GREEN}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: CHART_GREEN }}
              />
              <Line
                type="monotone"
                dataKey="simNum"
                stroke={CHART_BLUE}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: CHART_BLUE }}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* ── Profitability heatmap (7×24) ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">
          Profitability by Day &amp; Hour (last 7 days)
        </h2>
        <p className="text-xs text-gray-500 mb-4">
          Average net spread bps per UTC hour. Darker green = more profitable on average. Grey = no data.
        </p>
        {heatmapLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : heatmapRaw.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No data in window</div>
        ) : (
          <div className="overflow-x-auto">
            <div style={{ display: 'grid', gridTemplateColumns: '36px repeat(24, 26px)', gap: 2 }}>
              <div />
              {HOURS_OF_DAY.map(h => (
                <div key={`hdr-${h}`} className="text-center text-gray-600" style={{ fontSize: 9 }}>
                  {h}
                </div>
              ))}
              {DAYS_OF_WEEK.flatMap((day, dow) => [
                <div key={`lbl-${dow}`} className="text-xs text-gray-400 flex items-center justify-end pr-1">
                  {day}
                </div>,
                ...HOURS_OF_DAY.map(hour => {
                  const cell = heatmapMap.get(`${dow}-${hour}`);
                  const intensity = cell ? Math.min(cell.avgNetSpreadBps / heatmapMax, 1) : 0;
                  const bg = cell
                    ? `rgba(52, 211, 153, ${(intensity * 0.85 + 0.15).toFixed(2)})`
                    : '#1f2937';
                  return (
                    <div
                      key={`${dow}-${hour}`}
                      className="rounded-sm"
                      style={{ width: 26, height: 26, backgroundColor: bg }}
                      title={
                        cell
                          ? `${day} ${hour}:00 UTC — ${cell.avgNetSpreadBps.toFixed(1)} bps avg · ${cell.count} opps`
                          : `${day} ${hour}:00 UTC — no data`
                      }
                    />
                  );
                }),
              ])}
            </div>
          </div>
        )}
      </div>

      {/* ── False positive rate over time ── */}
      <div className="rounded-lg border border-surface-border bg-surface-raised p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-1">
          False Positive Rate (last {WINDOW_HOURS}h)
        </h2>
        <p className="text-xs text-gray-500 mb-4">
          % of resolved opportunities that were unprofitable (expired before execution, or simulated as a loss).
          Lower is better — indicates the detection engine is generating clean, capturable signals.
        </p>
        {fprLoading ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">Loading…</div>
        ) : fprData.length === 0 ? (
          <div className="h-48 flex items-center justify-center text-gray-500 text-sm">No resolved opportunities in window</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={fprData} margin={{ top: 4, right: 8, left: 4, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={CHART_MUTED} />
              <XAxis
                dataKey="hour"
                tickFormatter={formatHourLabel}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                domain={[0, 100]}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => `${v}%`}
              />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#d1d5db', fontSize: 11 }}
                labelFormatter={(label) => formatHourLabel(String(label))}
                formatter={(value: unknown, _name: unknown, entry) => [
                  `${typeof value === 'number' ? value.toFixed(1) : String(value)}% (${(entry.payload as { falsePositiveCount: number; totalCount: number }).falsePositiveCount}/${(entry.payload as { totalCount: number }).totalCount})`,
                  'False Positive Rate',
                ]}
              />
              <ReferenceLine y={50} stroke={CHART_MUTED} strokeDasharray="3 3" />
              <Line
                type="monotone"
                dataKey="falsePositiveRate"
                stroke={CHART_RED}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: CHART_RED }}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
