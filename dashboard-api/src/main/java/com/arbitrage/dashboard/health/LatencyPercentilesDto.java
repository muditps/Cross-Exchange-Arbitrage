package com.arbitrage.dashboard.health;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST response carrying p50/p95/p99/p999 latency percentiles per pipeline stage,
 * served by {@link LatencyController}.
 *
 * <p>All values are in milliseconds. The frontend uses these to render the pipeline
 * latency waterfall chart. Values are 0 until the first 10-second measurement interval
 * completes after startup.
 *
 * @param stages map of stage name (e.g. {@code "END_TO_END"}) to its percentile snapshot
 */
public record LatencyPercentilesDto(
        Map<String, StagePercentiles> stages
) {

    /**
     * Percentile values for one pipeline stage, in milliseconds.
     *
     * @param p50Ms   50th percentile (median) latency in milliseconds
     * @param p95Ms   95th percentile latency in milliseconds
     * @param p99Ms   99th percentile latency in milliseconds
     * @param p999Ms  99.9th percentile (tail) latency in milliseconds
     */
    public record StagePercentiles(double p50Ms, double p95Ms, double p99Ms, double p999Ms) {}

    /**
     * Builds a {@link LatencyPercentilesDto} from the raw nanosecond map produced by
     * {@link com.arbitrage.detection.service.LatencyMetricsPublisher#getLatestPercentiles()}.
     *
     * @param rawNanos map of stage name → (percentile name → value in nanoseconds)
     * @return response DTO with all values converted to milliseconds
     */
    public static LatencyPercentilesDto from(Map<String, Map<String, Long>> rawNanos) {
        final Map<String, StagePercentiles> stages = new LinkedHashMap<>();
        rawNanos.forEach((stage, percentiles) ->
                stages.put(stage, new StagePercentiles(
                        percentiles.getOrDefault("p50", 0L) / 1_000_000.0,
                        percentiles.getOrDefault("p95", 0L) / 1_000_000.0,
                        percentiles.getOrDefault("p99", 0L) / 1_000_000.0,
                        percentiles.getOrDefault("p999", 0L) / 1_000_000.0)));
        return new LatencyPercentilesDto(stages);
    }
}
