package com.arbitrage.detection.service;

import com.arbitrage.common.model.LatencyContext;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LatencyRecorder}.
 *
 * <p>Verifies that segment durations are correctly derived from the T0–T9 timestamp chain
 * and recorded into the right HdrHistogram stage buckets.
 */
@DisplayName("LatencyRecorder")
class LatencyRecorderTest {

    private LatencyRecorder recorder;

    @BeforeEach
    void setUp() {
        recorder = new LatencyRecorder();
    }

    @Test
    @DisplayName("record with complete context populates all six stage histograms")
    void record_completeContext_populatesAllStages() {
        LatencyContext ctx = buildCompleteContext(
                1_000_000L,   // T1
                2_000_000L,   // T2
                5_000_000L,   // T3 (transit T3-T2 = 3ms)
                6_000_000L,   // T4
                7_000_000L,   // T5 (normaliser processing T5-T3 = 2ms)
                10_000_000L,  // T6 (transit T6-T5 = 3ms)
                12_000_000L,  // T7
                15_000_000L,  // T8 (detection T8-T6 = 5ms)
                16_000_000L   // T9 (publish T9-T8 = 1ms, end-to-end T9-T1 = 15ms)
        );

        recorder.record(ctx);

        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            Histogram snap = recorder.getIntervalHistogram(stage);
            assertThat(snap.getTotalCount())
                    .as("stage %s should have 1 recorded value", stage)
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("record with T1 == 0 (no connector timestamp) skips all stages")
    void record_missingT1_skipsAllStages() {
        LatencyContext ctx = LatencyContext.builder()
                .t1ParseStart(0L)
                .t9OpportunityPublished(16_000_000L)
                .build();

        recorder.record(ctx);

        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            assertThat(recorder.getIntervalHistogram(stage).getTotalCount())
                    .as("stage %s should have no values when T1 is missing", stage)
                    .isEqualTo(0L);
        }
    }

    @Test
    @DisplayName("record with T9 == 0 (no opportunity published) skips all stages")
    void record_missingT9_skipsAllStages() {
        LatencyContext ctx = LatencyContext.builder()
                .t1ParseStart(1_000_000L)
                .t9OpportunityPublished(0L)
                .build();

        recorder.record(ctx);

        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            assertThat(recorder.getIntervalHistogram(stage).getTotalCount())
                    .as("stage %s should have no values when T9 is missing", stage)
                    .isEqualTo(0L);
        }
    }

    @Test
    @DisplayName("getIntervalHistogram resets the accumulator — second call returns empty histogram")
    void getIntervalHistogram_advancesInterval_subsequentCallIsEmpty() {
        recorder.record(buildCompleteContext(
                1_000_000L, 2_000_000L, 5_000_000L, 6_000_000L,
                7_000_000L, 10_000_000L, 12_000_000L, 15_000_000L, 16_000_000L));

        Histogram first = recorder.getIntervalHistogram(LatencyRecorder.Stage.END_TO_END);
        Histogram second = recorder.getIntervalHistogram(LatencyRecorder.Stage.END_TO_END);

        assertThat(first.getTotalCount()).isEqualTo(1L);
        assertThat(second.getTotalCount()).isEqualTo(0L);
    }

    @Test
    @DisplayName("end-to-end segment is T9 - T1 in nanoseconds")
    void record_endToEnd_isT9MinusT1() {
        long t1 = 1_000_000L;
        long t9 = 16_000_000L; // 15ms end-to-end
        recorder.record(buildCompleteContext(t1, 2_000_000L, 5_000_000L, 6_000_000L,
                7_000_000L, 10_000_000L, 12_000_000L, 15_000_000L, t9));

        Histogram snap = recorder.getIntervalHistogram(LatencyRecorder.Stage.END_TO_END);

        assertThat(snap.getTotalCount()).isEqualTo(1L);
        // HdrHistogram guarantees ≤0.1% error; for 15ms (15_000_000ns) the p100 is within that tolerance
        assertThat(snap.getValueAtPercentile(100.0)).isBetween(t9 - t1 - 15_000L, t9 - t1 + 15_000L);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private LatencyContext buildCompleteContext(long t1, long t2, long t3, long t4,
                                               long t5, long t6, long t7, long t8, long t9) {
        return LatencyContext.builder()
                .t0WebSocketReceived(0L)
                .t1ParseStart(t1)
                .t2RawTickPublished(t2)
                .t3NormalisationReceived(t3)
                .t4NormalisationComplete(t4)
                .t5NormalisedTickPublished(t5)
                .t6DetectionReceived(t6)
                .t7RedisStateUpdated(t7)
                .t8ComparisonComplete(t8)
                .t9OpportunityPublished(t9)
                .build();
    }
}
