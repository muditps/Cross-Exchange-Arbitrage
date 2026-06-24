package com.arbitrage.dashboard.health;

import com.arbitrage.detection.service.LatencyMetricsPublisher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoints exposing pipeline latency percentiles measured by HdrHistogram.
 *
 * <p><strong>Why this exists:</strong> Prometheus gauges update on a pull model — the frontend
 * cannot query them directly without a Prometheus/Grafana stack. This endpoint exposes the same
 * data via a simple REST call so the dashboard Health page can display latency numbers without
 * requiring Grafana to be running.
 *
 * <p><strong>Data source:</strong> {@link LatencyMetricsPublisher} snapshots
 * {@link com.arbitrage.detection.service.LatencyRecorder} HdrHistogram recorders every 10
 * seconds. This controller reads the last computed snapshot — it does NOT trigger a new
 * histogram snapshot on each HTTP request.
 *
 * <p><strong>Why {@link LatencyMetricsPublisher} is available here:</strong>
 * {@code dashboard-api} includes {@code detection-engine} as a compile dependency; all
 * {@code @Component} beans from that module are registered in the shared Spring context.
 */
@RestController
@RequestMapping("/api/latency")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Latency", description = "HdrHistogram pipeline latency percentiles per stage")
public class LatencyController {

    private final LatencyMetricsPublisher latencyMetricsPublisher;

    /**
     * Returns the current p50/p95/p99/p999 latency percentiles for each pipeline stage.
     *
     * <p>Values are in milliseconds, derived from the most recent 10-second HdrHistogram
     * snapshot. All values are 0 until the first snapshot interval completes after startup
     * (typically 10–15 seconds after the first opportunity is detected).
     *
     * <p>Pipeline stages: {@code CONNECTOR_TO_NORMALISER_TRANSIT},
     * {@code NORMALISER_PROCESSING}, {@code NORMALISER_TO_DETECTOR_TRANSIT},
     * {@code DETECTION_PROCESSING}, {@code PUBLISH_OVERHEAD}, {@code END_TO_END}.
     *
     * @return DTO containing all stage percentile values in milliseconds
     */
    @GetMapping("/percentiles")
    @Operation(
            summary = "Pipeline latency percentiles",
            description = "Returns p50/p95/p99/p999 (ms) for each pipeline stage from HdrHistogram. " +
                    "Values are 0 until the first 10-second measurement interval completes."
    )
    @ApiResponse(responseCode = "200", description = "Latency percentiles for all pipeline stages")
    public LatencyPercentilesDto getLatencyPercentiles() {
        return LatencyPercentilesDto.from(latencyMetricsPublisher.getLatestPercentiles());
    }
}
