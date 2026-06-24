/**
 * Domain types for the Multi-Asset Cross-Exchange Arbitrage Detection Platform.
 *
 * These mirror the Java common-models exactly. All numeric fields that are
 * BigDecimal in Java are string here — JSON serialisation of BigDecimal
 * preserves full precision only as a string (number loses significant digits
 * beyond 15 significant figures for crypto prices at 8dp).
 */

// ── Enums ────────────────────────────────────────────────────────────────────

export type ExchangeId = 'BINANCE' | 'BYBIT' | 'KUCOIN';

export type OpportunityStatus = 'DETECTED' | 'OPEN' | 'CLOSED' | 'EXPIRED';

// ── Value objects ─────────────────────────────────────────────────────────────

export interface TradingPair {
  baseCurrency: string;
  quoteCurrency: string;
}

/** Returns canonical symbol e.g. "BTC-USDT". */
export function canonicalSymbol(pair: TradingPair): string {
  return `${pair.baseCurrency}-${pair.quoteCurrency}`;
}

// ── Core stream types ─────────────────────────────────────────────────────────

/**
 * Real-time best bid/ask from one exchange for one pair.
 * Emitted by normalisation-engine onto the normalised-ticks Kafka topic.
 *
 * Numeric fields are strings to preserve BigDecimal precision from the API.
 * Use parseFloat() only for display; use a library like decimal.js for arithmetic.
 */
export interface NormalisedTick {
  exchangeId: ExchangeId;
  tradingPair: TradingPair;
  bestBidPrice: string;
  bestAskPrice: string;
  bestBidQuantity: string;
  bestAskQuantity: string;
  exchangeTimestamp: string;
  receivedTimestamp: number;
  processedTimestamp: number;
}

/**
 * A detected cross-exchange arbitrage opportunity in any lifecycle state.
 * Emitted by detection-engine onto the arbitrage-opportunities Kafka topic.
 */
export interface ArbitrageOpportunity {
  id: string;
  tradingPair: TradingPair;
  buyExchange: ExchangeId;
  buyPrice: string;
  buyQuantity: string;
  buyFeeRate: string;
  sellExchange: ExchangeId;
  sellPrice: string;
  sellQuantity: string;
  sellFeeRate: string;
  grossSpread: string;
  netSpread: string;
  grossSpreadBps: string;
  netSpreadBps: string;
  arbitrageableQuantity: string;
  theoreticalProfit: string;
  status: OpportunityStatus;
  detectionTimestamp: string;
  lastUpdateTimestamp: string;
  detectedNanoTime: number;
  closedNanoTime: number;
  peakNetSpread: string;
  averageNetSpread: string;
  totalDurationMs: number;
  updateCount: number;
}

/**
 * Simulated execution outcome for a CLOSED opportunity.
 * Persisted by execution-simulator to TimescaleDB.
 */
export interface SimulationResult {
  id: string;
  opportunityId: string;
  simulatedLatencyMs: number;
  slippageBps: string;
  simulatedBuyPrice: string;
  simulatedSellPrice: string;
  simulatedQuantity: string;
  wasProfitable: boolean;
  netProfit: string;
  fillProbability: string;
  simulationTimestamp: string;
}

// ── WebSocket message envelopes ───────────────────────────────────────────────

/** Generic envelope for WebSocket push messages from dashboard-api. */
export interface WsMessage<T> {
  type: WsMessageType;
  payload: T;
  timestamp: string;
}

export type WsMessageType =
  | 'TICK_UPDATE'
  | 'OPPORTUNITY_UPDATE'
  | 'HEALTH_UPDATE'
  | 'SIMULATION_RESULT';

// ── Health REST response types ────────────────────────────────────────────────

/** Feed health snapshot for one exchange. Returned by GET /api/health/exchanges. */
export interface ExchangeHealth {
  exchangeId: string;
  status: 'CONNECTED' | 'STALE' | 'DISCONNECTED' | 'RECONNECTING';
  lastTickAgeMs: number | null;
  isConnected: boolean;
}

/**
 * Spring Actuator /actuator/health response shape (show-details: always).
 * We parse components.infrastructure.details for Kafka/Redis/TimescaleDB status.
 */
export interface ActuatorHealthComponent {
  status: 'UP' | 'DOWN' | 'UNKNOWN';
  details?: Record<string, unknown>;
}

export interface ActuatorHealth {
  status: 'UP' | 'DOWN' | 'UNKNOWN';
  components?: Record<string, ActuatorHealthComponent>;
}

// ── Analytics REST response types ────────────────────────────────────────────

/**
 * Aggregate opportunity + simulation stats for a given time window.
 * Returned by GET /api/analytics/summary.
 */
export interface AnalyticsSummary {
  totalOpportunities: number;
  closedOpportunities: number;
  expiredOpportunities: number;
  activeOpportunities: number;
  avgNetSpreadBps: number;
  maxNetSpreadBps: number;
  totalSimulations: number;
  profitableSimulations: number;
  winRatePct: number;
  windowHours: number;
}

/**
 * Opportunity count for one hour bucket.
 * Returned by GET /api/analytics/hourly.
 */
export interface HourlyCount {
  hour: string;   // ISO-8601 e.g. "2026-06-07T14:00:00Z"
  count: number;
}

/**
 * Opportunity stats grouped by buy/sell exchange route.
 * Returned by GET /api/analytics/by-exchange-pair.
 */
export interface ExchangePairStats {
  buyExchange: string;
  sellExchange: string;
  count: number;
  avgNetSpreadBps: number;
}

// ── Historical opportunity types (REST, paginated) ───────────────────────────

/**
 * One row from the arbitrage_opportunities TimescaleDB table.
 * Column names differ slightly from the live ArbitrageOpportunity:
 *  rawSpreadBps (DB: raw_spread_bps) = grossSpread before fees
 *  quantity (DB: quantity) = arbitrageableQuantity
 *  estimatedProfit (DB: estimated_profit) = theoreticalProfit
 */
export interface ClosedOpportunity {
  id: string;
  tradingPair: string;
  buyExchange: string;
  sellExchange: string;
  buyPrice: string;
  sellPrice: string;
  rawSpreadBps: string;
  netSpreadBps: string;
  buyFeeRate: string;
  sellFeeRate: string;
  quantity: string;
  estimatedProfit: string;
  status: 'CLOSED' | 'EXPIRED';
  detectionTimestamp: string;
  closedTimestamp: string | null;
  peakNetSpreadBps: string | null;
  averageNetSpreadBps: string | null;
  totalDurationMs: number;
}

/** Execution simulation outcome for a closed opportunity. */
export interface SimulationSummary {
  simulatedLatencyMs: number;
  slippageBps: string;
  simulatedBuyPrice: string;
  simulatedSellPrice: string;
  simulatedQuantity: string;
  wasProfitable: boolean;
  netProfit: string;
  fillProbability: string;
}

/** Paginated response from GET /api/opportunities. */
export interface OpportunityPage {
  opportunities: ClosedOpportunity[];
  totalCount: number;
  page: number;
  size: number;
}

// ── JVM and Kafka metrics (Spring Actuator) ───────────────────────────────────

/**
 * Raw Spring Actuator /actuator/metrics/{metric} response shape.
 * measurements[0].value holds the current scalar value for simple gauges.
 */
export interface ActuatorMetricResponse {
  name: string;
  measurements: { statistic: string; value: number }[];
}

/**
 * Processed JVM memory and thread metrics polled from Spring Actuator.
 * isAvailable = false when the backend is unreachable or metrics are not exposed.
 */
export interface JvmMetrics {
  heapUsedBytes: number;
  heapMaxBytes: number;
  liveThreads: number;
  isAvailable: boolean;
}

/**
 * Kafka consumer lag metrics polled from Spring Actuator Micrometer.
 * maxLag is the maximum records-lag-max across all consumer instances.
 * isAvailable = false when the metric is not yet registered (no consumer has started).
 */
export interface KafkaLagMetrics {
  maxLag: number;
  isAvailable: boolean;
}

// ── Analytics chart data types ───────────────────────────────────────────────

/** One bucket in the net-spread distribution histogram (Chart 2). */
export interface SpreadBucket {
  bucketLabel: string;
  count: number;
}

/** One bucket in the opportunity duration distribution histogram (Chart 3). */
export interface DurationBucket {
  bucketLabel: string;
  count: number;
}

/**
 * One point on the cumulative P&L line chart (Chart 4).
 * Both profit values are strings — BigDecimal serialised to preserve 8dp precision.
 */
export interface CumulativePnlPoint {
  ts: string;
  cumulativeGrossProfit: string;
  cumulativeSimProfit: string;
}

/**
 * One data point on the Latency vs Profitability scatter chart.
 * X = simulated execution latency (ms); Y = net profit after fees and slippage.
 * The frontend computes a linear regression trendline and breakeven latency annotation.
 */
export interface LatencyProfitabilityPoint {
  latencyMs: number;
  netProfit: string;
}

/**
 * One cell in the 7×24 profitability heatmap.
 * dayOfWeek: 0=Sunday…6=Saturday (PostgreSQL DOW convention).
 * Cells absent from the response have no opportunities and are rendered grey.
 */
export interface HeatmapCell {
  dayOfWeek: number;
  hourOfDay: number;
  avgNetSpreadBps: number;
  count: number;
}

/**
 * One hourly data point on the False Positive Rate chart.
 * falsePositiveRate is pre-computed as falsePositiveCount / totalCount × 100 (0–100).
 */
export interface FalsePositiveRatePoint {
  hour: string;
  totalCount: number;
  falsePositiveCount: number;
  falsePositiveRate: number;
}

// ── Latency percentiles (from GET /api/latency/percentiles) ──────────────────

/** p50/p95/p99/p999 for one pipeline stage, in milliseconds. */
export interface StagePercentiles {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  p999Ms: number;
}

/**
 * Response from GET /api/latency/percentiles.
 * Keys are stage names: CONNECTOR_TO_NORMALISER_TRANSIT, NORMALISER_PROCESSING,
 * NORMALISER_TO_DETECTOR_TRANSIT, DETECTION_PROCESSING, PUBLISH_OVERHEAD, END_TO_END.
 * All values in milliseconds. Returns 0 until the first 10s HdrHistogram interval completes.
 */
export interface LatencyPercentilesResponse {
  stages: Record<string, StagePercentiles>;
}

// ── UI-specific types ─────────────────────────────────────────────────────────

export type WsConnectionStatus = 'CONNECTING' | 'CONNECTED' | 'DISCONNECTED' | 'ERROR';

export interface ExchangeHealthState {
  exchangeId: ExchangeId;
  isConnected: boolean;
  lastTickAgeMs: number;
  ticksPerSecond: number;
}

/** Display-ready price row for the live monitor table. */
export interface PriceTableRow {
  exchangeId: ExchangeId;
  tradingPair: string;
  bid: string;
  ask: string;
  spread: string;
  lastUpdateMs: number;
  isStale: boolean;
}
