import { useState, useEffect, useCallback } from 'react';
import { useExchangeHealth, useSystemHealth, useJvmMetrics, useKafkaLag } from '@/hooks/useHealthData';
import type { ExchangeHealth } from '@/types';

// ── Data Management ───────────────────────────────────────────────────────────

const PAIRS = ['BTC-USDT', 'ETH-USDT', 'BNB-USDT'] as const;
type CleanupScope = `pair:${string}` | 'all';

interface DeleteResult {
  scope: string;
  opportunitiesDeleted: number;
  simulationsDeleted: number;
}

/**
 * Admin panel for resetting TimescaleDB data per-pair or globally.
 * Uses a two-click confirmation pattern to prevent accidental deletes.
 * Confirmation auto-cancels after 4 seconds of inactivity.
 */
function DataManagementSection() {
  const [pendingConfirm, setPendingConfirm] = useState<CleanupScope | null>(null);
  const [isDeleting, setIsDeleting]         = useState(false);
  const [lastResult, setLastResult]         = useState<string | null>(null);

  // Auto-cancel pending confirmation after 4 s
  useEffect(() => {
    if (!pendingConfirm) return;
    const timer = setTimeout(() => setPendingConfirm(null), 4000);
    return () => clearTimeout(timer);
  }, [pendingConfirm]);

  const handleClick = useCallback(async (scope: CleanupScope) => {
    if (pendingConfirm !== scope) {
      setPendingConfirm(scope);
      return;
    }
    // Second click — confirmed
    setPendingConfirm(null);
    setIsDeleting(true);
    setLastResult(null);
    try {
      const url = scope === 'all'
        ? '/api/admin/data/all'
        : `/api/admin/data/pair/${scope.replace('pair:', '')}`;
      const res = await fetch(url, { method: 'DELETE' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: DeleteResult = await res.json();
      setLastResult(
        `Deleted ${data.opportunitiesDeleted.toLocaleString()} opportunities and ` +
        `${data.simulationsDeleted.toLocaleString()} simulations` +
        (data.scope === 'ALL' ? ' across all pairs.' : ` for ${data.scope}.`)
      );
    } catch (err) {
      setLastResult(`Delete failed: ${err instanceof Error ? err.message : 'unknown error'}`);
    } finally {
      setIsDeleting(false);
    }
  }, [pendingConfirm]);

  const buttonLabel = (scope: CleanupScope, defaultLabel: string) =>
    pendingConfirm === scope ? 'Confirm?' : defaultLabel;

  const buttonClass = (scope: CleanupScope, base: string) =>
    pendingConfirm === scope
      ? `${base} border-red-500 text-red-400 hover:bg-red-500/10`
      : `${base} border-surface-border text-gray-400 hover:border-gray-500 hover:text-gray-200`;

  return (
    <section>
      <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-1">
        Data Management
      </h2>
      <p className="text-xs text-gray-500 mb-4">
        Deletes from TimescaleDB (arbitrage_opportunities + simulation_results). Irreversible.
        Click once to arm, click again within 4 s to confirm.
      </p>

      {/* Per-pair buttons */}
      <div className="flex flex-wrap gap-3 mb-4">
        {PAIRS.map((pair) => {
          const scope: CleanupScope = `pair:${pair}`;
          return (
            <button
              key={pair}
              disabled={isDeleting}
              onClick={() => handleClick(scope)}
              className={buttonClass(scope,
                'px-4 py-2 rounded-md border text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed'
              )}
            >
              {isDeleting && pendingConfirm === null
                ? '…'
                : buttonLabel(scope, `Clear ${pair}`)}
            </button>
          );
        })}
      </div>

      {/* Full reset */}
      <button
        disabled={isDeleting}
        onClick={() => handleClick('all')}
        className={buttonClass('all',
          'px-4 py-2 rounded-md border text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed'
        )}
      >
        {isDeleting ? 'Deleting…' : buttonLabel('all', 'Clear All Data')}
      </button>

      {/* Result message */}
      {lastResult && (
        <p className={`mt-3 text-xs ${lastResult.startsWith('Delete failed') ? 'text-accent-red' : 'text-accent-green'}`}>
          {lastResult}
        </p>
      )}
    </section>
  );
}

// ── Status styling ────────────────────────────────────────────────────────────

type FeedStatus = ExchangeHealth['status'];

const FEED_STATUS_STYLES: Record<FeedStatus, { dot: string; badge: string }> = {
  CONNECTED:    { dot: 'bg-accent-green', badge: 'text-accent-green' },
  RECONNECTING: { dot: 'bg-accent-yellow', badge: 'text-accent-yellow' },
  STALE:        { dot: 'bg-amber-400', badge: 'text-amber-400' },
  DISCONNECTED: { dot: 'bg-accent-red', badge: 'text-accent-red' },
};

const INFRA_UP_STYLES   = { dot: 'bg-accent-green', text: 'text-accent-green' };
const INFRA_DOWN_STYLES = { dot: 'bg-accent-red',   text: 'text-accent-red'   };

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatAge(ageMs: number | null): string {
  if (ageMs === null) return 'Never seen';
  if (ageMs < 1_000) return `${ageMs}ms ago`;
  if (ageMs < 60_000) return `${(ageMs / 1_000).toFixed(1)}s ago`;
  return `${Math.floor(ageMs / 60_000)}m ago`;
}

// ── Sub-components ────────────────────────────────────────────────────────────

interface ExchangeCardProps { exchange: ExchangeHealth; }

function ExchangeCard({ exchange }: ExchangeCardProps) {
  const styles = FEED_STATUS_STYLES[exchange.status];
  return (
    <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
        {exchange.exchangeId}
      </p>
      <div className="flex items-center gap-2 mb-2">
        <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${styles.dot}`} />
        <span className={`text-sm font-semibold ${styles.badge}`}>{exchange.status}</span>
      </div>
      <p className="text-xs text-gray-500">{formatAge(exchange.lastTickAgeMs)}</p>
    </div>
  );
}

interface InfraCardProps {
  label: string;
  isUp: boolean | undefined;
  loading: boolean;
}

function InfraCard({ label, isUp, loading }: InfraCardProps) {
  const styles = isUp ? INFRA_UP_STYLES : INFRA_DOWN_STYLES;
  const statusText = loading ? '…' : (isUp ? 'UP' : 'DOWN');
  return (
    <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">{label}</p>
      <div className="flex items-center gap-2">
        {!loading && (
          <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${styles.dot}`} />
        )}
        <span className={`text-sm font-semibold ${loading ? 'text-gray-500' : styles.text}`}>
          {statusText}
        </span>
      </div>
    </div>
  );
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 MB';
  return `${Math.round(bytes / 1024 / 1024)} MB`;
}

function lagColor(maxLag: number): string {
  if (maxLag < 10)  return 'text-accent-green';
  if (maxLag < 100) return 'text-amber-400';
  return 'text-accent-red';
}

function lagLabel(maxLag: number): string {
  if (maxLag < 10)  return 'Healthy';
  if (maxLag < 100) return 'Elevated';
  return 'High — consumer falling behind';
}

// ── Page ──────────────────────────────────────────────────────────────────────

/**
 * Displays the health of all exchange feeds, infrastructure components,
 * JVM memory + thread state, and Kafka consumer lag.
 *
 * Exchange feed health: 5s refresh (stale threshold = 5s on the backend).
 * Infrastructure health: 10s refresh via Spring Actuator /actuator/health.
 * JVM metrics: 10s refresh via Spring Actuator /actuator/metrics/*.
 * Kafka consumer lag: 10s refresh via Micrometer kafka.consumer.records-lag-max.
 *
 * Pipeline latency waterfall and HdrHistogram histogram are deferred to
 * Phase 6 where T0-T9 instrumentation and HdrHistogram recording are added.
 */
export function HealthPage() {
  const { data: exchanges = [], isLoading: exchangesLoading, isError: exchangesError } = useExchangeHealth();
  const { data: systemHealth, isLoading: systemLoading } = useSystemHealth();
  const { isLoading: jvmLoading, data: jvm } = useJvmMetrics();
  const { isLoading: lagLoading, data: lag } = useKafkaLag();

  const infraDetails = systemHealth?.components?.['infrastructure']?.details ?? {};
  const kafkaUp      = infraDetails['kafka']      === 'UP';
  const redisUp      = infraDetails['redis']      === 'UP';
  const timescaleUp  = infraDetails['timescaledb'] === 'UP';

  return (
    <div className="p-6 space-y-8">
      <div>
        <h1 className="text-2xl font-semibold text-white mb-1">System Health</h1>
        <p className="text-gray-400 text-sm">
          Exchange feed status (5s refresh) and infrastructure liveness (10s refresh).
        </p>
      </div>

      {/* ── Exchange feed health ── */}
      <section>
        <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          Exchange Feeds
        </h2>
        {exchangesError && (
          <div className="mb-3 rounded-md bg-red-500/10 border border-red-500/30 px-4 py-2 text-sm text-red-300">
            Failed to load exchange health — is the backend running?
          </div>
        )}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          {exchangesLoading
            ? ['BINANCE', 'BYBIT', 'KUCOIN'].map((id) => (
                <div key={id} className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4 animate-pulse">
                  <div className="h-3 bg-gray-700 rounded w-20 mb-3" />
                  <div className="h-4 bg-gray-700 rounded w-24 mb-2" />
                  <div className="h-3 bg-gray-700 rounded w-16" />
                </div>
              ))
            : exchanges.map((exchange) => (
                <ExchangeCard key={exchange.exchangeId} exchange={exchange} />
              ))
          }
        </div>
      </section>

      {/* ── Infrastructure health ── */}
      <section>
        <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          Infrastructure
        </h2>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <InfraCard label="Kafka"       isUp={systemLoading ? undefined : kafkaUp}      loading={systemLoading} />
          <InfraCard label="Redis"       isUp={systemLoading ? undefined : redisUp}      loading={systemLoading} />
          <InfraCard label="TimescaleDB" isUp={systemLoading ? undefined : timescaleUp}  loading={systemLoading} />
        </div>
      </section>

      {/* ── JVM Metrics ── */}
      <section>
        <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          JVM
        </h2>
        {jvmLoading ? (
          <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4 animate-pulse">
            <div className="h-3 bg-gray-700 rounded w-32 mb-3" />
            <div className="h-2 bg-gray-700 rounded w-full mb-2" />
            <div className="h-3 bg-gray-700 rounded w-24" />
          </div>
        ) : !jvm.isAvailable ? (
          <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4 text-sm text-gray-500">
            JVM metrics unavailable — ensure <code className="text-gray-400">management.endpoints.web.exposure.include=*</code> is set
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            {/* Heap usage */}
            <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Heap Memory</p>
              <p className="text-sm font-mono text-white mb-2">
                {formatBytes(jvm.heapUsedBytes)}
                <span className="text-gray-500 text-xs ml-1">/ {formatBytes(jvm.heapMaxBytes)}</span>
              </p>
              {jvm.heapMaxBytes > 0 && (() => {
                const pct = Math.round(jvm.heapUsedBytes / jvm.heapMaxBytes * 100);
                const barColor = pct > 80 ? 'bg-accent-red' : pct > 60 ? 'bg-amber-400' : 'bg-accent-green';
                return (
                  <>
                    <div className="h-2 bg-gray-700 rounded-full overflow-hidden">
                      <div className={`h-full rounded-full transition-all ${barColor}`} style={{ width: `${pct}%` }} />
                    </div>
                    <p className="text-xs text-gray-500 mt-1">{pct}% used</p>
                  </>
                );
              })()}
            </div>
            {/* Threads */}
            <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Live Threads</p>
              <p className="text-2xl font-mono font-semibold text-white">{jvm.liveThreads}</p>
              <p className="text-xs text-gray-500 mt-0.5">
                Includes Netty I/O, Kafka consumers, and boundedElastic workers
              </p>
            </div>
          </div>
        )}
      </section>

      {/* ── Kafka Consumer Lag ── */}
      <section>
        <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          Kafka Consumer Lag
        </h2>
        {lagLoading ? (
          <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4 animate-pulse">
            <div className="h-3 bg-gray-700 rounded w-24 mb-3" />
            <div className="h-6 bg-gray-700 rounded w-16" />
          </div>
        ) : !lag.isAvailable ? (
          <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4 text-sm text-gray-500">
            Consumer lag metric not yet available — starts reporting after the first Kafka poll
          </div>
        ) : (
          <div className="rounded-lg border border-surface-border bg-surface-raised px-5 py-4">
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Max Records Lag</p>
            <div className="flex items-baseline gap-2">
              <span className={`text-3xl font-mono font-semibold ${lagColor(lag.maxLag)}`}>
                {lag.maxLag}
              </span>
              <span className="text-xs text-gray-500">msgs</span>
            </div>
            <p className={`text-xs mt-1 ${lagColor(lag.maxLag)}`}>{lagLabel(lag.maxLag)}</p>
            <p className="text-xs text-gray-600 mt-2">
              Green &lt;10 · Yellow 10–100 · Red &gt;100
            </p>
          </div>
        )}
      </section>

      {/* ── Data Management ── */}
      <DataManagementSection />

      <p className="text-xs text-gray-600">
        Exchange feeds: <span className="text-accent-green">●</span> CONNECTED &nbsp;
        <span className="text-amber-400">●</span> STALE &nbsp;
        <span className="text-accent-red">●</span> DISCONNECTED &nbsp;
        <span className="text-accent-yellow">●</span> RECONNECTING
      </p>
    </div>
  );
}
