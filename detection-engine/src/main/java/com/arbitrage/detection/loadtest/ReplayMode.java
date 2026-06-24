package com.arbitrage.detection.loadtest;

/**
 * Configures the injection rate for a {@link TickReplayTool} load-test run.
 *
 * <p>Each mode simulates a different market condition:
 * <ul>
 *   <li>{@link #REALTIME} — mirrors the actual production tick rate (~90 ticks/sec across
 *       3 exchanges × 3 pairs). Used as the baseline to verify the pipeline is healthy
 *       before stressing it.</li>
 *   <li>{@link #FAST_5X} — 5× production rate. Tests headroom before the pipeline shows
 *       signs of strain.</li>
 *   <li>{@link #FAST_10X} — 10× production rate. Typical "performance cliff" zone — where
 *       p99 crosses the 100ms threshold. This is the key number for the performance report.</li>
 *   <li>{@link #BURST} — 1,000 messages injected in 100ms (10,000 msgs/sec peak), then 5s
 *       pause before repeating. Models a market microstructure event: a flash crash, a large
 *       order book sweep, or a news-driven price spike where all exchanges move simultaneously.
 *       The pause lets the system drain the queue before the next burst, allowing repeated
 *       measurement of burst recovery time.</li>
 * </ul>
 *
 * <p><b>Why these specific rates:</b> The live system produces approximately 90 ticks/sec
 * in production (3 exchanges × 3 pairs × ~10 ticks/sec per pair). FAST_10X = 900/sec is
 * the saturation target. Beyond that, Kafka consumer lag compounds faster than the detection
 * engine can drain it, causing ticks to be stale before processing.
 */
public enum ReplayMode {

    /**
     * ~90 ticks/sec — mirrors live production rate.
     * Baseline health check; should produce p99 &lt; 50ms with zero consumer lag.
     */
    REALTIME(90, "Baseline: mirrors live production rate (~90 ticks/sec)"),

    /**
     * ~450 ticks/sec — 5× production rate.
     * Tests pipeline headroom; p99 is expected to remain under 100ms.
     */
    FAST_5X(450, "5× production rate (~450 ticks/sec)"),

    /**
     * ~900 ticks/sec — 10× production rate.
     * Performance cliff zone; p99 crossing 100ms threshold is the key datapoint.
     */
    FAST_10X(900, "10× production rate (~900 ticks/sec)"),

    /**
     * 1,000 msgs / 100ms burst, then 5s pause.
     * Peak injection: 10,000 msgs/sec. Models market microstructure events.
     * Measures burst recovery time: how long until p99 returns to baseline after the burst.
     */
    BURST(-1, "Burst: 1000 msgs in 100ms, then 5s pause (10,000 msgs/sec peak)");

    /** Sustained ticks per second. -1 for BURST (rate is not constant). */
    private final int ticksPerSecond;

    /** Human-readable description for logs and REST responses. */
    private final String description;

    ReplayMode(int ticksPerSecond, String description) {
        this.ticksPerSecond = ticksPerSecond;
        this.description = description;
    }

    /**
     * Returns the sustained injection rate in ticks per second.
     * Returns -1 for {@link #BURST} — use {@link #isBurst()} to detect this case.
     *
     * @return ticks per second, or -1 for burst mode
     */
    public int getTicksPerSecond() {
        return ticksPerSecond;
    }

    /** @return human-readable description for logs and REST responses */
    public String getDescription() {
        return description;
    }

    /** @return true if this is the variable-rate burst mode */
    public boolean isBurst() {
        return this == BURST;
    }
}
