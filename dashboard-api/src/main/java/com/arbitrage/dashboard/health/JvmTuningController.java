package com.arbitrage.dashboard.health;

import com.arbitrage.detection.loadtest.JvmExperimentResult;
import com.arbitrage.detection.loadtest.ReplayMode;
import com.arbitrage.detection.loadtest.ReplayResult;
import com.arbitrage.detection.loadtest.TickReplayTool;
import com.arbitrage.detection.service.GcSnapshot;
import com.arbitrage.detection.service.GcStatsService;
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

import java.lang.management.ManagementFactory;

/**
 * REST interface for JVM GC monitoring and tuning experiments.
 *
 * <p>Two endpoints:
 * <ul>
 *   <li>{@code GET /api/jvm-tuning/gc-stats} — current heap + GC type snapshot. Use to confirm
 *       the server started with the expected GC config before running an experiment.</li>
 *   <li>{@code POST /api/jvm-tuning/run-experiment} — runs a full load test and captures the
 *       GC delta (events + pause time) that occurred during the test window.</li>
 * </ul>
 *
 * <p><b>6-config experiment workflow:</b>
 * <ol>
 *   <li>Set {@code JVM_GC_ARGS} env var (e.g. {@code -XX:+UseG1GC -Xmx512m -Xms256m})</li>
 *   <li>Start: {@code ./gradlew :dashboard-api:bootRun}</li>
 *   <li>Confirm config: {@code GET /api/jvm-tuning/gc-stats} → verify {@code gcType}</li>
 *   <li>Run: {@code POST /api/jvm-tuning/run-experiment?configName=G1GC_512MB&mode=REALTIME}</li>
 *   <li>Record p50/p99, GC events, and GC time from the response JSON</li>
 *   <li>Stop server, change {@code JVM_GC_ARGS}, repeat for all 6 configs</li>
 * </ol>
 *
 * <p>The 6 configurations to run for Phase 6 Session 6.5:
 * <ol>
 *   <li>{@code G1GC_512MB}: -XX:+UseG1GC -Xmx512m -Xms256m</li>
 *   <li>{@code G1GC_2GB}: -XX:+UseG1GC -Xmx2g -Xms1g</li>
 *   <li>{@code ZGC_512MB}: -XX:+UseZGC -Xmx512m -Xms256m</li>
 *   <li>{@code ZGC_2GB}: -XX:+UseZGC -Xmx2g -Xms1g</li>
 *   <li>{@code G1GC_PRETOUCH}: -XX:+UseG1GC -Xmx2g -Xms2g -XX:+AlwaysPreTouch</li>
 *   <li>{@code ZGC_PRETOUCH}: -XX:+UseZGC -Xmx2g -Xms2g -XX:+AlwaysPreTouch</li>
 * </ol>
 */
@RestController
@RequestMapping("/api/jvm-tuning")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "JVM Tuning", description = "JVM GC monitoring and Phase 6 Session 6.5 tuning experiment runner")
public class JvmTuningController {

    private final GcStatsService gcStatsService;
    private final TickReplayTool tickReplayTool;

    /**
     * Returns a point-in-time snapshot of the JVM's active GC algorithm and heap state.
     *
     * <p>Call this before running an experiment to confirm the server started with the
     * expected GC config (check {@code gcType} field matches what you set in {@code JVM_GC_ARGS}).
     *
     * @return current {@link GcSnapshot} with GC type, heap usage, and per-collector stats
     */
    @GetMapping("/gc-stats")
    @Operation(
            summary = "Current JVM GC snapshot",
            description = "Returns the active GC algorithm, heap usage (used/max in MB), " +
                    "and per-collector statistics. Use this to confirm the JVM started with " +
                    "the expected GC config before running an experiment.")
    @ApiResponse(responseCode = "200", description = "Current JVM GC snapshot")
    public ResponseEntity<GcSnapshot> getGcStats() {
        return ResponseEntity.ok(gcStatsService.captureSnapshot());
    }

    /**
     * Runs a JVM tuning experiment: captures GC state before/after a full load test run.
     *
     * <p>Blocks for {@code durationSeconds + 12s} (the 12s settle period is fixed in
     * {@link TickReplayTool} for HdrHistogram snapshot accuracy). Returns 409 if a load
     * test is already in progress.
     *
     * <p>The response includes:
     * <ul>
     *   <li>{@code gcEventsDuringTest} — how many GC collections occurred during the test</li>
     *   <li>{@code gcTimeDuringTestMs} — cumulative GC pause ms during the test (-1 for ZGC)</li>
     *   <li>{@code latencyResult} — full before/after HdrHistogram percentile snapshot</li>
     * </ul>
     *
     * @param configName      short label for this configuration (e.g. "G1GC_2GB", "ZGC_512MB")
     * @param mode            the replay injection rate profile
     * @param durationSeconds injection duration in seconds, not including 12s settle time (default: 60)
     * @return full experiment result with GC delta and latency percentiles, or 409 if already running
     */
    @PostMapping("/run-experiment")
    @Operation(
            summary = "Run JVM tuning experiment",
            description = "Captures GC state before and after a load test. Blocks for durationSeconds + 12s. " +
                    "Returns 409 if a test is already running. Set JVM_GC_ARGS env var before starting " +
                    "the server to configure the GC algorithm and heap size for this run.")
    @ApiResponse(responseCode = "200", description = "Experiment complete — full result returned")
    @ApiResponse(responseCode = "409", description = "Load test already in progress")
    public ResponseEntity<JvmExperimentResult> runExperiment(
            @Parameter(description = "Identifier for this config (e.g. G1GC_2GB, ZGC_512MB)", required = true)
            @RequestParam String configName,
            @Parameter(description = "Injection rate profile", required = true)
            @RequestParam ReplayMode mode,
            @Parameter(description = "Injection duration in seconds (not including 12s settle)", example = "60")
            @RequestParam(defaultValue = "60") int durationSeconds) {

        log.info("JVM tuning experiment requested: configName={} mode={} durationSeconds={}",
                configName, mode, durationSeconds);

        if (tickReplayTool.isRunning()) {
            return ResponseEntity.status(409).build();
        }

        GcSnapshot gcBefore = gcStatsService.captureSnapshot();
        log.info("Experiment starting — GC snapshot before: gcType={} heapUsed={}MB heapMax={}MB gcCount={}",
                gcBefore.gcType(), gcBefore.heapUsedMb(), gcBefore.heapMaxMb(), gcBefore.totalGcCount());

        final ReplayResult latencyResult = tickReplayTool.runLoadTest(mode, durationSeconds);
        if (latencyResult == null) {
            return ResponseEntity.status(409).build();
        }

        GcSnapshot gcAfter = gcStatsService.captureSnapshot();
        long gcEventsDelta = gcAfter.totalGcCount() - gcBefore.totalGcCount();
        log.info("Experiment complete — GC snapshot after: gcType={} heapUsed={}MB gcEventsDelta={}",
                gcAfter.gcType(), gcAfter.heapUsedMb(), gcEventsDelta);

        JvmExperimentResult result = JvmExperimentResult.of(
                configName,
                ManagementFactory.getRuntimeMXBean().getInputArguments(),
                gcBefore,
                gcAfter,
                latencyResult
        );

        log.info("Experiment result: configName={} gcEvents={} gcTimeMs={} detectionP99AfterMs={}ms endToEndP99AfterMs={}ms",
                configName, result.gcEventsDuringTest(), result.gcTimeDuringTestMs(),
                result.detectionP99AfterMs(), latencyResult.endToEndP99AfterMs());

        return ResponseEntity.ok(result);
    }
}
