package com.arbitrage.detection.service;

/**
 * Immutable snapshot of a single GarbageCollectorMXBean's statistics at one point in time.
 *
 * <p>By capturing this before and after a load test window, the delta reveals how many
 * GC collections occurred and how much cumulative pause time was spent during that window.
 *
 * <p><b>ZGC caveat:</b> For ZGC, {@code collectionTimeMs} is always -1. ZGC is a concurrent
 * collector — it has no STW "pause time" to report via MXBeans. Use -Xlog:gc* to capture
 * per-pause timing from GC logs if precise ZGC pause measurement is needed.
 *
 * @param name              GC collector name as reported by the JVM
 *                          (e.g. "G1 Young Generation", "G1 Old Generation",
 *                           "ZGC Cycles", "ZGC Pauses", "Shenandoah Cycles")
 * @param collectionCount   cumulative number of GC collections since JVM start (-1 if unsupported)
 * @param collectionTimeMs  cumulative GC pause time in ms since JVM start (-1 for ZGC)
 */
public record GcBeanStats(
        String name,
        long collectionCount,
        long collectionTimeMs
) {}
