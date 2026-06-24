/**
 * Pure analytics utilities for the dashboard's chart computations.
 * Kept in a separate module so they can be unit-tested independently of React.
 */

export interface TrendPoint {
  x: number;
  y: number;
}

/**
 * Fits an Ordinary Least Squares (OLS) linear regression to the given points
 * and returns the two endpoints of the trendline (at minX and maxX).
 *
 * OLS minimises the sum of squared vertical residuals — the vertical distances
 * from each data point to the fitted line. The closed-form solution is:
 *
 *   slope     m = (nΣxy − ΣxΣy) / (nΣx² − (Σx)²)
 *   intercept b = (Σy − mΣx) / n
 *
 * This is O(n) — four running sums computed in a single pass over the data.
 *
 * In the HFT context (Latency vs Profitability chart):
 *   x = execution latency (ms)
 *   y = net profit ($)
 * A negative slope (m < 0) confirms the fundamental HFT relationship:
 * higher latency → lower expected profit. The steeper the slope, the more
 * time-sensitive the strategy.
 *
 * Returns null when:
 * - Fewer than 2 points (can't fit a line)
 * - All x-values are identical (vertical line — slope is undefined)
 */
export function computeTrendLine(points: TrendPoint[]): [TrendPoint, TrendPoint] | null {
  const n = points.length;
  if (n < 2) return null;

  const sumX   = points.reduce((s, p) => s + p.x, 0);
  const sumY   = points.reduce((s, p) => s + p.y, 0);
  const sumXY  = points.reduce((s, p) => s + p.x * p.y, 0);
  const sumXSq = points.reduce((s, p) => s + p.x * p.x, 0);

  const denom = n * sumXSq - sumX * sumX;
  if (denom === 0) return null; // all x values identical — vertical line

  const slope     = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;

  const minX = Math.min(...points.map(p => p.x));
  const maxX = Math.max(...points.map(p => p.x));

  return [
    { x: minX, y: slope * minX + intercept },
    { x: maxX, y: slope * maxX + intercept },
  ];
}

/**
 * Returns the x-intercept of the trendline — the latency at which expected
 * profit crosses zero (the "breakeven latency").
 *
 * Derivation: set y = 0 in y = mx + b  →  x = −b/m
 *
 * Only meaningful when:
 * - The slope is strictly negative (downward trend — profit falls with latency)
 * - The intercept is positive (the line starts above zero)
 * - The resulting x is positive (a negative breakeven latency is nonsensical)
 *
 * Returns null in all other cases. The rounded integer is returned (ms is
 * precise enough for a dashboard annotation — fractional ms adds no meaning).
 */
export function computeBreakevenLatencyMs(trendLine: [TrendPoint, TrendPoint] | null): number | null {
  if (!trendLine) return null;

  const [p1, p2] = trendLine;
  const slope = (p2.y - p1.y) / (p2.x - p1.x);
  if (slope >= 0) return null; // flat or upward trend — no finite downward breakeven

  const intercept = p1.y - slope * p1.x;
  const breakeven = -intercept / slope; // x = −b/m

  return breakeven > 0 ? Math.round(breakeven) : null;
}
