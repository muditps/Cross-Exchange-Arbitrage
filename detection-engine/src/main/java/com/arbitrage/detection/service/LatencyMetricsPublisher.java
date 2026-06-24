package com.arbitrage.detection.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodically snapshots {@link LatencyRecorder} histograms and publishes p50/p95/p99/p999
 * latency percentiles as Micrometer gauges, making them available in Prometheus and Grafana.
 *
 * <p><b>Why percentiles, never averages:</b> Average latency hides tail latency. If p50=5ms
 * and p99=500ms, the average might read 15ms — looks fine. But 1 in 100 requests takes 500ms.
 * In trading, that 1 request could be the one that misses the arbitrage window. Always report
 * percentiles for latency-sensitive systems.
 *
 * <p><b>Metric name:</b> {@code pipeline.latency.nanos} with tags:
 * <ul>
 *   <li>{@code stage} — lowercase stage name from {@link LatencyRecorder.Stage}</li>
 *   <li>{@code percentile} — one of {@code p50}, {@code p95}, {@code p99}, {@code p999}</li>
 * </ul>
 *
 * <p><b>Interval:</b> Gauges are updated every 10 seconds from the HdrHistogram
 * interval snapshot. Between updates, Prometheus scrapes return the last computed value.
 *
 * <p><b>REST access:</b> {@link #getLatestPercentiles()} exposes the same data for
 * {@link com.arbitrage.dashboard.health.LatencyController}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LatencyMetricsPublisher {

    private static final double[] PERCENTILE_VALUES = {50.0, 95.0, 99.0, 99.9};
    private static final String[] PERCENTILE_NAMES = {"p50", "p95", "p99", "p999"};

    private final LatencyRecorder latencyRecorder;
    private final MeterRegistry meterRegistry;

    /**
     * Current percentile values in nanoseconds, per stage × percentile name.
     * Updated by {@link #publishLatencyMetrics()} and read by Micrometer gauges
     * and {@link #getLatestPercentiles()}.
     */
    private final Map<LatencyRecorder.Stage, Map<String, AtomicLong>> currentPercentilesNanos =
            new EnumMap<>(LatencyRecorder.Stage.class);

    /**
     * Registers one Micrometer {@link Gauge} per (stage, percentile) combination at startup.
     * Each gauge is backed by an {@link AtomicLong} holding the current nanosecond value.
     * 24 gauges total: 6 stages × 4 percentiles.
     */
    @PostConstruct
    void registerGauges() {
        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            final Map<String, AtomicLong> stageMap = new LinkedHashMap<>();
            for (int i = 0; i < PERCENTILE_NAMES.length; i++) {
                final AtomicLong holder = new AtomicLong(0L);
                stageMap.put(PERCENTILE_NAMES[i], holder);
                Gauge.builder("pipeline.latency.nanos", holder, AtomicLong::get)
                        .tag("stage", stage.name().toLowerCase())
                        .tag("percentile", PERCENTILE_NAMES[i])
                        .description("Pipeline latency in nanoseconds at the given percentile")
                        .register(meterRegistry);
            }
            currentPercentilesNanos.put(stage, stageMap);
        }
        log.info("LatencyMetricsPublisher registered {} Micrometer gauges (6 stages × 4 percentiles)",
                LatencyRecorder.Stage.values().length * PERCENTILE_NAMES.length);
    }

    /**
     * Snapshots all HdrHistogram recorders and updates Micrometer gauges.
     *
     * <p>Runs every 10 seconds. Stages with zero samples in the interval are skipped —
     * their gauges retain the previous interval's values (Prometheus shows a flat line,
     * which correctly signals no new data). This happens when there are no detected
     * opportunities in the interval.
     */
    @Scheduled(fixedDelay = 10_000)
    public void publishLatencyMetrics() {
        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            final Histogram snapshot = latencyRecorder.getIntervalHistogram(stage);
            if (snapshot.getTotalCount() == 0) {
                continue;
            }
            final Map<String, AtomicLong> stageMap = currentPercentilesNanos.get(stage);
            for (int i = 0; i < PERCENTILE_NAMES.length; i++) {
                stageMap.get(PERCENTILE_NAMES[i])
                        .set(snapshot.getValueAtPercentile(PERCENTILE_VALUES[i]));
            }
            log.info("Latency [{} samples] stage={}: p50={}ms p95={}ms p99={}ms p999={}ms",
                    snapshot.getTotalCount(), stage.name(),
                    stageMap.get("p50").get() / 1_000_000.0,
                    stageMap.get("p95").get() / 1_000_000.0,
                    stageMap.get("p99").get() / 1_000_000.0,
                    stageMap.get("p999").get() / 1_000_000.0);
        }
    }

    /**
     * Returns the most recently computed percentile values in nanoseconds for all stages.
     *
     * <p>Called by {@link com.arbitrage.dashboard.health.LatencyController} to expose current
     * latency metrics via the REST API. Returns 0 for stages with no recorded data yet.
     *
     * @return map of stage name → (percentile name → value in nanoseconds)
     */
    public Map<String, Map<String, Long>> getLatestPercentiles() {
        final Map<String, Map<String, Long>> result = new LinkedHashMap<>();
        for (LatencyRecorder.Stage stage : LatencyRecorder.Stage.values()) {
            final Map<String, AtomicLong> stageMap = currentPercentilesNanos.get(stage);
            final Map<String, Long> values = new LinkedHashMap<>();
            for (final String name : PERCENTILE_NAMES) {
                values.put(name, stageMap.get(name).get());
            }
            result.put(stage.name(), values);
        }
        return result;
    }
}
