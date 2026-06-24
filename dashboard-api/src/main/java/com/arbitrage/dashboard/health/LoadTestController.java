package com.arbitrage.dashboard.health;

import com.arbitrage.detection.loadtest.ReplayMode;
import com.arbitrage.detection.loadtest.ReplayResult;
import com.arbitrage.detection.loadtest.TickReplayTool;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST interface for the {@link TickReplayTool} load tester.
 *
 * <p>Allows triggering load tests via HTTP and checking whether a run is active.
 * All endpoints are synchronous: {@code POST /api/load-test/run} blocks until the
 * injection phase plus the post-run settle period ({@value #SETTLE_DESCRIPTION}) complete,
 * then returns the full before/after latency snapshot.
 *
 * <p><b>Typical usage:</b>
 * <ol>
 *   <li>Start with {@code REALTIME} (90/sec) to confirm baseline p99 &lt; 50ms.</li>
 *   <li>Run {@code FAST_5X} (450/sec) to confirm p99 still under 100ms.</li>
 *   <li>Run {@code FAST_10X} (900/sec) — if {@code performanceCliffDetected=true}, the cliff is here.</li>
 *   <li>Run {@code BURST} to measure burst recovery time (p99 spike then decay).</li>
 * </ol>
 */
@RestController
@RequestMapping("/api/load-test")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Load Test", description = "Inject synthetic ticks at configurable rates to stress-test the detection pipeline")
public class LoadTestController {

    private static final String SETTLE_DESCRIPTION = "12s post-injection settle";

    private final TickReplayTool tickReplayTool;

    /**
     * Triggers a load test run.
     *
     * <p>Blocks until injection completes + 12s settle time for HdrHistogram snapshot.
     * Returns 409 Conflict if a run is already in progress.
     *
     * @param mode            injection rate profile (REALTIME | FAST_5X | FAST_10X | BURST)
     * @param durationSeconds how long to inject, not including settle time (default: 60s)
     * @return the full before/after latency snapshot, or 409 if already running
     */
    @PostMapping("/run")
    @Operation(
            summary = "Run load test",
            description = "Injects synthetic ticks at the given rate for the given duration, " +
                    "then waits 12s for HdrHistogram snapshot before returning before/after p50/p99 per stage. " +
                    "Blocks for durationSeconds + 12s. Returns 409 if a run is already in progress.")
    @ApiResponse(responseCode = "200", description = "Run complete — latency snapshot returned")
    @ApiResponse(responseCode = "409", description = "Load test already in progress")
    public ResponseEntity<ReplayResult> runLoadTest(
            @Parameter(description = "Injection rate profile", required = true)
            @RequestParam ReplayMode mode,
            @Parameter(description = "Injection duration in seconds (not including 12s settle time)", example = "60")
            @RequestParam(defaultValue = "60") int durationSeconds) {

        log.info("Load test requested via REST: mode={} durationSeconds={}", mode, durationSeconds);

        final ReplayResult result = tickReplayTool.runLoadTest(mode, durationSeconds);
        if (result == null) {
            return ResponseEntity.status(409).build();
        }
        return ResponseEntity.ok(result);
    }

    /**
     * Returns whether a load test is currently running.
     *
     * @return {@code {"running": true/false}}
     */
    @GetMapping("/status")
    @Operation(summary = "Load test status", description = "Returns whether a load test is currently in progress")
    @ApiResponse(responseCode = "200", description = "Current load test status")
    public ResponseEntity<LoadTestStatusDto> getStatus() {
        return ResponseEntity.ok(new LoadTestStatusDto(tickReplayTool.isRunning()));
    }

    /**
     * Simple status DTO for the load test status endpoint.
     *
     * @param running true if a load test run is currently active
     */
    public record LoadTestStatusDto(boolean running) {}
}
