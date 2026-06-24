package com.arbitrage.detection.loadtest;

import java.util.Map;

/**
 * Captures the outcome of a single {@link TickReplayTool} load-test run.
 *
 * <p>Contains both the injection statistics (what we sent) and the latency
 * percentiles observed before and after the run (what the pipeline experienced).
 * Comparing before/after snapshots isolates the load test's impact on tail latency.
 *
 * <p><b>Why before AND after snapshots:</b> HdrHistogram's {@code Recorder} accumulates
 * values between {@code getIntervalHistogram()} calls. The "before" snapshot is the
 * 10-second window immediately before the run (baseline), and the "after" snapshot
 * is the window captured after the run completes. This shows how latency shifts under
 * sustained load vs idle.
 *
 * <p><b>Performance cliff detection:</b> When {@code endToEndP99AfterMs} crosses 100ms
 * while {@code endToEndP99BeforeMs} is below it, the run has found the performance cliff.
 *
 * @param mode                       the replay mode used for this run
 * @param requestedDurationSeconds   how long the run was configured to run
 * @param actualDurationMs           actual wall-clock duration of the injection phase
 * @param ticksProduced              total number of synthetic ticks sent to Kafka
 * @param actualThroughputPerSecond  achieved throughput (ticksProduced / actualDurationMs * 1000)
 * @param percentilesBeforeNanos     p50/p95/p99/p999 per stage BEFORE the run (nanos)
 * @param percentilesAfterNanos      p50/p95/p99/p999 per stage AFTER the run (nanos)
 * @param endToEndP99BeforeMs        convenience field: END_TO_END p99 before the run (ms)
 * @param endToEndP99AfterMs         convenience field: END_TO_END p99 after the run (ms)
 * @param performanceCliffDetected   true if end-to-end p99 crossed the 100ms threshold
 */
public record ReplayResult(
        ReplayMode mode,
        int requestedDurationSeconds,
        long actualDurationMs,
        long ticksProduced,
        double actualThroughputPerSecond,
        Map<String, Map<String, Long>> percentilesBeforeNanos,
        Map<String, Map<String, Long>> percentilesAfterNanos,
        double endToEndP99BeforeMs,
        double endToEndP99AfterMs,
        boolean performanceCliffDetected
) {

    private static final double PERFORMANCE_CLIFF_THRESHOLD_MS = 100.0;

    /**
     * Constructs a {@code ReplayResult} and derives {@code performanceCliffDetected}
     * from the before/after p99 values relative to the 100ms threshold.
     */
    public static ReplayResult of(
            ReplayMode mode,
            int requestedDurationSeconds,
            long actualDurationMs,
            long ticksProduced,
            Map<String, Map<String, Long>> beforeNanos,
            Map<String, Map<String, Long>> afterNanos) {

        final double p99BeforeMs = extractEndToEndP99Ms(beforeNanos);
        final double p99AfterMs = extractEndToEndP99Ms(afterNanos);

        return new ReplayResult(
                mode,
                requestedDurationSeconds,
                actualDurationMs,
                ticksProduced,
                actualDurationMs > 0 ? (double) ticksProduced / actualDurationMs * 1000.0 : 0.0,
                beforeNanos,
                afterNanos,
                p99BeforeMs,
                p99AfterMs,
                p99BeforeMs < PERFORMANCE_CLIFF_THRESHOLD_MS
                        && p99AfterMs >= PERFORMANCE_CLIFF_THRESHOLD_MS
        );
    }

    private static double extractEndToEndP99Ms(Map<String, Map<String, Long>> percentilesNanos) {
        final Map<String, Long> endToEnd = percentilesNanos.get("END_TO_END");
        if (endToEnd == null) {
            return 0.0;
        }
        final Long p99Nanos = endToEnd.get("p99");
        return p99Nanos != null ? p99Nanos / 1_000_000.0 : 0.0;
    }
}
