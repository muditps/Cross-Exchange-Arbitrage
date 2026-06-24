package com.arbitrage.detection.loadtest;

import com.arbitrage.detection.service.GcSnapshot;

import java.util.List;
import java.util.Map;

/**
 * Bundles the outcome of a single JVM tuning experiment: before/after GC state,
 * the GC delta that occurred during the test, and the full latency snapshot.
 *
 * <p>Use the static factory {@link #of} to construct an instance — it computes the
 * GC delta automatically by subtracting before from after snapshots.
 *
 * <p><b>Typical workflow:</b>
 * <ol>
 *   <li>Start the server with {@code JVM_GC_ARGS="-XX:+UseG1GC -Xmx2g"}</li>
 *   <li>POST {@code /api/jvm-tuning/run-experiment?configName=G1GC_2GB&mode=REALTIME}</li>
 *   <li>Record {@code gcEventsDuringTest}, {@code gcTimeDuringTestMs}, and
 *       {@code detectionP99AfterMs()} from the response</li>
 *   <li>Repeat for each of the 6 configs and compare</li>
 * </ol>
 *
 * <p><b>ZGC caveat:</b> {@code gcTimeDuringTestMs} will be -1 for ZGC because
 * {@link GcSnapshot#totalGcTimeMs()} returns -1 when any collector does not support
 * pause-time reporting (which ZGC doesn't). GC event count is still valid.
 *
 * @param configName           short identifier for this config (e.g. "G1GC_2GB", "ZGC_512MB")
 * @param jvmInputArguments    JVM args as reported by {@link java.lang.management.RuntimeMXBean#getInputArguments()}
 * @param gcBefore             GC snapshot captured immediately before the load test
 * @param gcAfter              GC snapshot captured after the load test and settle window
 * @param gcEventsDuringTest   delta of total GC collections between before and after snapshots
 * @param gcTimeDuringTestMs   delta of total GC pause time in ms between snapshots (-1 for ZGC)
 * @param latencyResult        the full HdrHistogram before/after percentile snapshot
 */
public record JvmExperimentResult(
        String configName,
        List<String> jvmInputArguments,
        GcSnapshot gcBefore,
        GcSnapshot gcAfter,
        long gcEventsDuringTest,
        long gcTimeDuringTestMs,
        ReplayResult latencyResult
) {

    /**
     * Constructs a {@code JvmExperimentResult} and computes the GC delta automatically.
     *
     * @param configName          short label for this configuration
     * @param jvmInputArguments   JVM flags from RuntimeMXBean
     * @param gcBefore            snapshot before the load test
     * @param gcAfter             snapshot after the load test
     * @param latencyResult       HdrHistogram percentile result from {@link TickReplayTool}
     * @return a fully populated experiment result
     */
    public static JvmExperimentResult of(
            String configName,
            List<String> jvmInputArguments,
            GcSnapshot gcBefore,
            GcSnapshot gcAfter,
            ReplayResult latencyResult) {

        long gcEventsDelta = gcAfter.totalGcCount() - gcBefore.totalGcCount();

        long gcTimeDelta;
        if (gcBefore.totalGcTimeMs() == -1 || gcAfter.totalGcTimeMs() == -1) {
            gcTimeDelta = -1;
        } else {
            gcTimeDelta = gcAfter.totalGcTimeMs() - gcBefore.totalGcTimeMs();
        }

        return new JvmExperimentResult(
                configName,
                List.copyOf(jvmInputArguments),
                gcBefore,
                gcAfter,
                gcEventsDelta,
                gcTimeDelta,
                latencyResult
        );
    }

    /**
     * Convenience: DETECTION_PROCESSING p99 after the run, in milliseconds.
     * Returns 0.0 if no data is available for this stage.
     *
     * @return p99 detection latency in ms, post-load-test
     */
    public double detectionP99AfterMs() {
        Map<String, Long> detectionPercentiles = latencyResult.percentilesAfterNanos()
                .get("DETECTION_PROCESSING");
        if (detectionPercentiles == null) {
            return 0.0;
        }
        Long p99Nanos = detectionPercentiles.get("p99");
        return p99Nanos != null ? p99Nanos / 1_000_000.0 : 0.0;
    }
}
