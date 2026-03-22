package com.arbitrage.common.model;

import lombok.Builder;
import lombok.Value;

/**
 * Carries nanosecond timestamps through the entire processing pipeline,
 * enabling precise measurement of each stage's contribution to end-to-end latency.
 *
 * <p><b>Why this exists:</b> You cannot optimise what you cannot measure. At HFT
 * firms, every microsecond matters. This context object lets us answer questions
 * like "where does our pipeline spend time?" and "which stage is the bottleneck?"
 * The latency breakdown waterfall chart on the dashboard (Phase 5) is powered
 * entirely by this object.
 *
 * <p><b>All timestamps use {@code System.nanoTime()}.</b> This is a monotonic clock
 * — it never jumps backward (unlike {@code System.currentTimeMillis()} which can
 * shift during NTP sync). The absolute value is meaningless; only the difference
 * between two nanoTime readings is meaningful.
 *
 * <p><b>Measurement points (T0–T9):</b>
 * <pre>
 * Exchange ──WebSocket──▶ [T0: message arrives]
 *                         [T1: JSON parsing starts]
 *                         [T2: published to raw Kafka topic]
 *           ──Kafka──▶    [T3: normalisation consumer receives]
 *                         [T4: normalisation transform complete]
 *                         [T5: published to normalised-ticks topic]
 *           ──Kafka──▶    [T6: detection consumer receives]
 *                         [T7: Redis state updated]
 *                         [T8: comparison logic complete]
 *                         [T9: opportunity published (if detected)]
 * </pre>
 *
 * <p><b>Key latency segments:</b>
 * <ul>
 *   <li>{@code T1 - T0} — WebSocket receive overhead</li>
 *   <li>{@code T2 - T1} — JSON parsing + Kafka produce</li>
 *   <li>{@code T4 - T3} — Normalisation transform</li>
 *   <li>{@code T5 - T4} — Normalised tick Kafka produce</li>
 *   <li>{@code T8 - T6} — Detection (Redis read + comparison)</li>
 *   <li>{@code T9 - T0} — End-to-end pipeline latency (the headline number)</li>
 * </ul>
 *
 * <p><b>Transport:</b> Carried as Kafka headers (not message payload) to avoid
 * polluting the business data schema. Headers are byte arrays; we encode each
 * timestamp as an 8-byte big-endian long.
 *
 * <p>Immutable ({@code @Value}) — same thread-safety rationale as {@link NormalisedTick}.
 */
@Value
@Builder(toBuilder = true)
public class LatencyContext {

    /**
     * T0 — {@code System.nanoTime()} captured the instant the WebSocket
     * {@code onMessage} callback fires. This MUST be the very first
     * operation in the callback — before logging, before parsing, before
     * anything. This timestamp is the ground truth for pipeline latency.
     */
    long t0WebSocketReceived;

    /** T1 — {@code System.nanoTime()} when JSON parsing begins. */
    long t1ParseStart;

    /** T2 — {@code System.nanoTime()} when the raw tick is published to Kafka. */
    long t2RawTickPublished;

    /** T3 — {@code System.nanoTime()} when the normalisation consumer receives the message. */
    long t3NormalisationReceived;

    /** T4 — {@code System.nanoTime()} when the normalisation transform completes. */
    long t4NormalisationComplete;

    /** T5 — {@code System.nanoTime()} when the normalised tick is published to Kafka. */
    long t5NormalisedTickPublished;

    /** T6 — {@code System.nanoTime()} when the detection consumer receives the message. */
    long t6DetectionReceived;

    /** T7 — {@code System.nanoTime()} when Redis price state is updated. */
    long t7RedisStateUpdated;

    /** T8 — {@code System.nanoTime()} when the comparison logic completes. */
    long t8ComparisonComplete;

    /** T9 — {@code System.nanoTime()} when an opportunity event is published (if any). */
    long t9OpportunityPublished;

    /**
     * Computes end-to-end pipeline latency in nanoseconds: {@code T9 - T0}.
     *
     * <p>This is the headline metric — the total time from WebSocket message
     * arrival to opportunity publication. Target: sub-100ms for the full pipeline.
     *
     * @return end-to-end latency in nanoseconds, or 0 if T9 has not been set
     */
    public long endToEndLatencyNanos() {
        if (t9OpportunityPublished == 0 || t0WebSocketReceived == 0) {
            return 0;
        }
        return t9OpportunityPublished - t0WebSocketReceived;
    }

    /**
     * Computes normalisation latency in nanoseconds: {@code T4 - T3}.
     *
     * @return normalisation latency in nanoseconds, or 0 if timestamps are not set
     */
    public long normalisationLatencyNanos() {
        if (t4NormalisationComplete == 0 || t3NormalisationReceived == 0) {
            return 0;
        }
        return t4NormalisationComplete - t3NormalisationReceived;
    }

    /**
     * Computes detection latency in nanoseconds: {@code T8 - T6}.
     * Includes Redis state update and comparison logic.
     *
     * @return detection latency in nanoseconds, or 0 if timestamps are not set
     */
    public long detectionLatencyNanos() {
        if (t8ComparisonComplete == 0 || t6DetectionReceived == 0) {
            return 0;
        }
        return t8ComparisonComplete - t6DetectionReceived;
    }
}
