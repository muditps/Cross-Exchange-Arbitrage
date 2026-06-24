package com.arbitrage.detection.service;

import java.util.List;

/**
 * Point-in-time snapshot of JVM memory and GC state.
 *
 * <p>Capture one snapshot before a load test and one after. Subtract the before/after
 * {@link #totalGcCount()} and {@link #totalGcTimeMs()} values to isolate GC events
 * that occurred strictly within the test window.
 *
 * <p><b>Why two helpers instead of storing the delta:</b> The snapshot is immutable and
 * reusable — {@code JvmExperimentResult} computes the delta from two snapshots so each
 * snapshot can be logged and inspected independently.
 *
 * @param jvmVersion   JVM version string from {@link java.lang.management.RuntimeMXBean#getVmVersion()}
 *                     (e.g. "21.0.10+9-LTS")
 * @param gcType       Detected GC algorithm: "G1GC", "ZGC", "Shenandoah", "ParallelGC", "SerialGC",
 *                     or "Unknown"
 * @param heapUsedMb   Current heap bytes in use, converted to MB
 * @param heapMaxMb    Maximum configured heap size in MB (reflects -Xmx)
 * @param gcBeans      Per-collector stats; one entry per {@link java.lang.management.GarbageCollectorMXBean}
 * @param capturedAtMs Wall-clock time when this snapshot was taken ({@link System#currentTimeMillis()})
 */
public record GcSnapshot(
        String jvmVersion,
        String gcType,
        long heapUsedMb,
        long heapMaxMb,
        List<GcBeanStats> gcBeans,
        long capturedAtMs
) {

    /**
     * Sums {@link GcBeanStats#collectionCount()} across all collectors.
     * Values of -1 (unsupported) are treated as 0 to avoid poisoning the sum.
     *
     * @return total GC event count since JVM start at the moment this snapshot was captured
     */
    public long totalGcCount() {
        return gcBeans.stream()
                .mapToLong(b -> Math.max(b.collectionCount(), 0L))
                .sum();
    }

    /**
     * Sums {@link GcBeanStats#collectionTimeMs()} across all collectors.
     *
     * <p>Returns -1 if ANY collector reports -1 (i.e. ZGC is active) because a partial
     * sum would be misleading — callers should not display a time figure in that case.
     *
     * @return total GC pause time in ms since JVM start, or -1 if unsupported (ZGC)
     */
    public long totalGcTimeMs() {
        boolean hasUnsupported = gcBeans.stream().anyMatch(b -> b.collectionTimeMs() == -1);
        if (hasUnsupported) {
            return -1;
        }
        return gcBeans.stream()
                .mapToLong(GcBeanStats::collectionTimeMs)
                .sum();
    }
}
