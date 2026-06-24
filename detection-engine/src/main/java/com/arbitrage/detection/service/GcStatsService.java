package com.arbitrage.detection.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.List;

/**
 * Reads live JVM GC statistics via the {@link java.lang.management} MXBean API.
 *
 * <p>Captures heap usage and per-collector event counts at any instant. Intended for
 * use before and after load tests to quantify GC overhead per JVM tuning configuration.
 *
 * <p><b>GC type detection:</b> The JVM registers {@link GarbageCollectorMXBean}s with
 * well-known names per GC algorithm. This service maps those names to short labels:
 * <ul>
 *   <li>G1GC: "G1 Young Generation" + "G1 Old Generation"</li>
 *   <li>ZGC: "ZGC" (JDK 17) or "ZGC Cycles" + "ZGC Pauses" (JDK 21+)</li>
 *   <li>Shenandoah: "Shenandoah Cycles" + "Shenandoah Pauses"</li>
 *   <li>ParallelGC: "PS Scavenge" + "PS MarkSweep"</li>
 *   <li>SerialGC: "Copy" + "MarkSweepCompact"</li>
 * </ul>
 *
 * <p><b>ZGC caveat:</b> ZGC's {@code getCollectionTime()} always returns -1. GC event count
 * is still valid; pause duration requires GC log parsing (-Xlog:gc*).
 */
@Service
@Slf4j
public class GcStatsService {

    private final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    /**
     * Captures the current JVM GC state as an immutable snapshot.
     *
     * <p>This call is very cheap — it reads from JVM MXBeans which are maintained
     * internally by the JVM with no compute cost at read time.
     *
     * @return a {@link GcSnapshot} with heap usage and per-collector event counts at this instant
     */
    public GcSnapshot captureSnapshot() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        List<GcBeanStats> beanStats = gcBeans.stream()
                .map(b -> new GcBeanStats(b.getName(), b.getCollectionCount(), b.getCollectionTime()))
                .toList();

        long heapUsedBytes = memoryBean.getHeapMemoryUsage().getUsed();
        long heapMaxBytes = memoryBean.getHeapMemoryUsage().getMax();

        String gcType = detectGcType(gcBeans);
        String jvmVersion = runtimeBean.getVmVersion();

        GcSnapshot snapshot = new GcSnapshot(
                jvmVersion,
                gcType,
                heapUsedBytes / (1024L * 1024L),
                heapMaxBytes / (1024L * 1024L),
                beanStats,
                System.currentTimeMillis()
        );

        log.debug("GC snapshot captured: gcType={} heapUsed={}MB heapMax={}MB collectors={}",
                gcType, snapshot.heapUsedMb(), snapshot.heapMaxMb(), beanStats.size());

        return snapshot;
    }

    /**
     * Inspects active GC bean names and returns a short human-readable label for the algorithm.
     *
     * @param gcBeans the JVM's active GC collector beans
     * @return "G1GC", "ZGC", "Shenandoah", "ParallelGC", "SerialGC", or "Unknown"
     */
    private String detectGcType(List<GarbageCollectorMXBean> gcBeans) {
        String allNames = gcBeans.stream()
                .map(GarbageCollectorMXBean::getName)
                .reduce("", (a, b) -> a + " " + b)
                .toLowerCase();

        if (allNames.contains("zgc")) {
            return "ZGC";
        } else if (allNames.contains("g1")) {
            return "G1GC";
        } else if (allNames.contains("shenandoah")) {
            return "Shenandoah";
        } else if (allNames.contains("ps scavenge") || allNames.contains("ps marksweep")) {
            return "ParallelGC";
        } else if (allNames.contains("copy") || allNames.contains("marksweepcompact")) {
            return "SerialGC";
        }
        return "Unknown";
    }
}
