import { describe, it, expect } from 'vitest';
import { computeTrendLine, computeBreakevenLatencyMs, type TrendPoint } from './analytics';

// ── computeTrendLine ──────────────────────────────────────────────────────────

describe('computeTrendLine', () => {
  it('returns null for an empty array', () => {
    expect(computeTrendLine([])).toBeNull();
  });

  it('returns null for a single point', () => {
    expect(computeTrendLine([{ x: 10, y: 5 }])).toBeNull();
  });

  it('returns null when all x-values are identical (vertical line — undefined slope)', () => {
    const points: TrendPoint[] = [{ x: 5, y: 1 }, { x: 5, y: 3 }, { x: 5, y: 7 }];
    expect(computeTrendLine(points)).toBeNull();
  });

  it('fits a perfect downward slope: y = −x + 10 (HFT scenario — profit falls with latency)', () => {
    const points: TrendPoint[] = [
      { x: 0,  y: 10 },
      { x: 5,  y: 5  },
      { x: 10, y: 0  },
    ];
    const result = computeTrendLine(points);
    expect(result).not.toBeNull();
    // trendline endpoints are at minX=0 and maxX=10
    expect(result![0].x).toBe(0);
    expect(result![0].y).toBeCloseTo(10, 5);
    expect(result![1].x).toBe(10);
    expect(result![1].y).toBeCloseTo(0, 5);
  });

  it('fits a perfect upward slope: y = 2x', () => {
    const points: TrendPoint[] = [
      { x: 1, y: 2 },
      { x: 2, y: 4 },
      { x: 3, y: 6 },
    ];
    const result = computeTrendLine(points);
    expect(result).not.toBeNull();
    expect(result![0].y).toBeCloseTo(2, 5); // y at x=1
    expect(result![1].y).toBeCloseTo(6, 5); // y at x=3
  });

  it('fits a flat line (zero slope): all y = 5', () => {
    const points: TrendPoint[] = [
      { x: 1, y: 5 },
      { x: 2, y: 5 },
      { x: 3, y: 5 },
    ];
    const result = computeTrendLine(points);
    expect(result).not.toBeNull();
    expect(result![0].y).toBeCloseTo(5, 5);
    expect(result![1].y).toBeCloseTo(5, 5);
  });

  it('works with exactly two points', () => {
    const points: TrendPoint[] = [{ x: 0, y: 100 }, { x: 100, y: 0 }];
    const result = computeTrendLine(points);
    expect(result).not.toBeNull();
    expect(result![0]).toEqual({ x: 0, y: 100 });
    expect(result![1]).toEqual({ x: 100, y: 0 });
  });

  it('handles noisy data — result endpoints are on the fitted line', () => {
    // y ≈ −0.5x + 50 with noise
    const points: TrendPoint[] = [
      { x: 10, y: 46 },
      { x: 20, y: 41 },
      { x: 30, y: 35 },
      { x: 40, y: 29 },
      { x: 50, y: 24 },
    ];
    const result = computeTrendLine(points);
    expect(result).not.toBeNull();
    // Both endpoints should be on the fitted line (x=10 and x=50)
    // Slope ≈ -0.55, intercept ≈ 51.5 — not asserting exact values, just that it's a downward trend
    expect(result![1].y).toBeLessThan(result![0].y);
  });
});

// ── computeBreakevenLatencyMs ─────────────────────────────────────────────────

describe('computeBreakevenLatencyMs', () => {
  it('returns null for a null trendline', () => {
    expect(computeBreakevenLatencyMs(null)).toBeNull();
  });

  it('returns null for a flat line (zero slope — no breakeven)', () => {
    const flat: [TrendPoint, TrendPoint] = [{ x: 0, y: 5 }, { x: 100, y: 5 }];
    expect(computeBreakevenLatencyMs(flat)).toBeNull();
  });

  it('returns null for an upward slope (profit increases with latency — no finite breakeven below x=∞)', () => {
    const rising: [TrendPoint, TrendPoint] = [{ x: 0, y: 0 }, { x: 100, y: 50 }];
    expect(computeBreakevenLatencyMs(rising)).toBeNull();
  });

  it('computes breakeven correctly for y = −x + 100 → breakeven at x = 100', () => {
    // Line from (0, 100) to (200, -100): slope = -1, intercept = 100
    const trendLine: [TrendPoint, TrendPoint] = [{ x: 0, y: 100 }, { x: 200, y: -100 }];
    expect(computeBreakevenLatencyMs(trendLine)).toBe(100);
  });

  it('computes breakeven correctly for y = −2x + 60 → breakeven at x = 30', () => {
    // Line from (0, 60) to (50, -40): slope = -2, intercept = 60
    const trendLine: [TrendPoint, TrendPoint] = [{ x: 0, y: 60 }, { x: 50, y: -40 }];
    expect(computeBreakevenLatencyMs(trendLine)).toBe(30);
  });

  it('returns null when breakeven is negative (line starts below zero and slopes down)', () => {
    // y = −x − 5: breakeven at x = −5 (negative — nonsensical latency)
    const trendLine: [TrendPoint, TrendPoint] = [{ x: 0, y: -5 }, { x: 10, y: -15 }];
    expect(computeBreakevenLatencyMs(trendLine)).toBeNull();
  });

  it('returns a rounded integer (not fractional ms)', () => {
    // y = −3x + 100: breakeven at x = 33.333... → rounds to 33
    const trendLine: [TrendPoint, TrendPoint] = [{ x: 0, y: 100 }, { x: 100, y: -200 }];
    const result = computeBreakevenLatencyMs(trendLine);
    expect(result).not.toBeNull();
    expect(Number.isInteger(result)).toBe(true);
    expect(result).toBe(33); // Math.round(100/3) = 33
  });
});
