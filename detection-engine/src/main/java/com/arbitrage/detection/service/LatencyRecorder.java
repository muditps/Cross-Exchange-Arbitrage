package com.arbitrage.detection.service;

import com.arbitrage.common.model.LatencyContext;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

/**
 * Records nanosecond-precision pipeline latency into HdrHistogram {@link Recorder} instances,
 * one per pipeline stage.
 *
 * <p><b>Why HdrHistogram:</b> Standard percentile implementations bucket values into wide
 * ranges, causing "HDR aliasing" — a 10ms value and a 15ms value might land in the same
 * bucket and both report as "12ms". HdrHistogram guarantees at most 0.1% value error across
 * the full range (1ns to 60s here), using only ~40KB of RAM per recorder. It is the library
 * used in production at LMAX, Aeron, and real HFT infrastructure.
 *
 * <p><b>Recording pattern:</b> Uses HdrHistogram's {@link Recorder} — a two-histogram flip
 * mechanism. The recording thread writes to one histogram; the reader calls
 * {@link #getIntervalHistogram(Stage)} to atomically swap and return the accumulated values
 * since the last call. This is Gil Tene's recommended pattern for concurrent recording with
 * periodic snapshotting at no lock contention cost.
 *
 * <p><b>Thread safety:</b> {@link Recorder#recordValue} is thread-safe. Multiple Kafka
 * consumer threads recording concurrently is safe. {@link #getIntervalHistogram} is intended
 * for exclusive use by {@link LatencyMetricsPublisher}'s scheduled thread.
 *
 * <p><b>Pipeline stages measured (T0–T9 chain from {@link LatencyContext}):</b>
 * <pre>
 *   CONNECTOR_TO_NORMALISER_TRANSIT  = T3 − T2
 *   NORMALISER_PROCESSING            = T5 − T3
 *   NORMALISER_TO_DETECTOR_TRANSIT   = T6 − T5
 *   DETECTION_PROCESSING             = T8 − T6
 *   PUBLISH_OVERHEAD                 = T9 − T8
 *   END_TO_END                       = T9 − T1
 * </pre>
 */
@Component
@Slf4j
public class LatencyRecorder {

    /**
     * Pipeline stages for which latency is recorded.
     * Each stage represents one measurable segment of the T0–T9 timestamp chain.
     */
    public enum Stage {
        /** T3 − T2: Kafka transit from connector producer to normalisation consumer. */
        CONNECTOR_TO_NORMALISER_TRANSIT,
        /** T5 − T3: Time inside the normalisation engine (consume, transform, produce). */
        NORMALISER_PROCESSING,
        /** T6 − T5: Kafka transit from normalisation producer to detection consumer. */
        NORMALISER_TO_DETECTOR_TRANSIT,
        /** T8 − T6: Time inside the detection engine (Redis state update + comparison). */
        DETECTION_PROCESSING,
        /** T9 − T8: Kafka produce overhead after the trading decision is made. */
        PUBLISH_OVERHEAD,
        /** T9 − T1: Full end-to-end latency from connector parse-start to opportunity published. */
        END_TO_END
    }

    /** Maximum trackable value: 60 seconds in nanoseconds. */
    private static final long MAX_TRACKABLE_NANOS = 60_000_000_000L;
    /** Three significant digits guarantees at most 0.1% value error. */
    private static final int SIGNIFICANT_DIGITS = 3;

    private final Map<Stage, Recorder> recorders;

    /** Initialises one {@link Recorder} per pipeline stage. Called by Spring at startup. */
    public LatencyRecorder() {
        recorders = new EnumMap<>(Stage.class);
        for (Stage stage : Stage.values()) {
            recorders.put(stage, new Recorder(MAX_TRACKABLE_NANOS, SIGNIFICANT_DIGITS));
        }
        log.info("LatencyRecorder initialised: {} stages, maxTrackable=60s, significantDigits={}",
                Stage.values().length, SIGNIFICANT_DIGITS);
    }

    /**
     * Records all pipeline segment durations from a completed {@link LatencyContext}.
     *
     * <p>Skips recording if the context is incomplete: {@code T1 == 0} means the connector
     * did not stamp a parse-start time (backward-compatibility with pre-6.1 messages);
     * {@code T9 == 0} means no opportunity was published (all directions were unprofitable
     * or stale). Both are required for the end-to-end segment.
     *
     * @param ctx the fully assembled latency context with T0–T9 nanosecond timestamps
     */
    public void record(LatencyContext ctx) {
        if (ctx.getT1ParseStart() == 0L || ctx.getT9OpportunityPublished() == 0L) {
            return;
        }
        recordIfPositive(Stage.CONNECTOR_TO_NORMALISER_TRANSIT,
                ctx.getT3NormalisationReceived() - ctx.getT2RawTickPublished());
        recordIfPositive(Stage.NORMALISER_PROCESSING,
                ctx.getT5NormalisedTickPublished() - ctx.getT3NormalisationReceived());
        recordIfPositive(Stage.NORMALISER_TO_DETECTOR_TRANSIT,
                ctx.getT6DetectionReceived() - ctx.getT5NormalisedTickPublished());
        recordIfPositive(Stage.DETECTION_PROCESSING,
                ctx.getT8ComparisonComplete() - ctx.getT6DetectionReceived());
        recordIfPositive(Stage.PUBLISH_OVERHEAD,
                ctx.getT9OpportunityPublished() - ctx.getT8ComparisonComplete());
        recordIfPositive(Stage.END_TO_END,
                ctx.getT9OpportunityPublished() - ctx.getT1ParseStart());
    }

    /**
     * Returns a snapshot {@link Histogram} covering all values recorded since the previous
     * call to this method (the recording interval).
     *
     * <p>Atomically swaps the active histogram and resets the accumulator, so each call
     * returns exactly the values from one interval. Designed for exclusive use by
     * {@link LatencyMetricsPublisher} on a 10-second schedule.
     *
     * @param stage the pipeline stage to snapshot
     * @return the interval histogram; {@link Histogram#getTotalCount()} is 0 if no
     *         values were recorded in this interval
     */
    public Histogram getIntervalHistogram(Stage stage) {
        return recorders.get(stage).getIntervalHistogram();
    }

    private void recordIfPositive(Stage stage, long valueNanos) {
        if (valueNanos > 0) {
            recorders.get(stage).recordValue(Math.min(valueNanos, MAX_TRACKABLE_NANOS));
        }
    }
}
