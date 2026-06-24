package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Micrometer-based metrics for the Detection Engine, following the RED pattern:
 * Rate (opportunities detected/closed/expired per unit time), Errors (N/A — detection
 * has no error path beyond stale-skips which {@link StalenessFilter} tracks separately),
 * Duration (comparison latency + opportunity lifetime).
 *
 * <p><b>Metrics exposed:</b>
 * <ul>
 *   <li>{@code detection.opportunities.events} (Counter, tag: status) — rate of lifecycle
 *       events (detected/closed/expired). Prometheus: {@code rate(detection_opportunities_events_total[1m])}
 *       gives opportunities per minute.</li>
 *   <li>{@code detection.opportunity.net.spread.bps} (DistributionSummary) — net spread in bps
 *       at the moment of detection. Distribution of how "good" detected opportunities are.
 *       p50/p99 show typical vs exceptional spread quality.</li>
 *   <li>{@code detection.opportunity.duration.ms} (DistributionSummary, tag: status) — lifetime
 *       of closed/expired opportunities in milliseconds. How long do opportunities last? A p99
 *       of 50ms means most close within one tick cycle. A p99 of 30s suggests feed anomalies.</li>
 *   <li>{@code detection.comparison.latency} (Timer, tag: exchange) — time from detection
 *       consumer receive (T6) to comparison complete (T8), including Redis read + BigDecimal math.
 *       Target: p99 &lt; 5ms. Use this to identify if Redis RTT is dominating comparison time.</li>
 * </ul>
 *
 * <p><b>Note on {@code StalenessFilter} metrics:</b> The {@code detection.stale.skips} counter
 * (per-exchange) is registered by {@link StalenessFilter#recordStaleSkip} — not here.
 * This class focuses on opportunity-level outcomes, not per-direction filter decisions.
 */
@Component
@Slf4j
public class DetectionMetrics {

    static final String METRIC_OPPORTUNITY_EVENTS     = "detection.opportunities.events";
    static final String METRIC_NET_SPREAD_BPS         = "detection.opportunity.net.spread.bps";
    static final String METRIC_OPPORTUNITY_DURATION_MS = "detection.opportunity.duration.ms";
    static final String METRIC_COMPARISON_LATENCY     = "detection.comparison.latency";

    static final String TAG_STATUS   = "status";
    static final String TAG_EXCHANGE = "exchange";

    private final MeterRegistry meterRegistry;

    /**
     * Creates detection metrics. Spring auto-injects the Micrometer registry
     * (wired to Prometheus by Spring Boot Actuator).
     *
     * @param meterRegistry the Micrometer meter registry
     */
    public DetectionMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        log.info("DetectionMetrics initialised with registry: {}", meterRegistry.getClass().getSimpleName());
    }

    /**
     * Records a published lifecycle event (DETECTED, CLOSED, or EXPIRED).
     *
     * <p>Always increments the events counter. For DETECTED events, also samples
     * the net spread distribution. For CLOSED/EXPIRED events, also samples the
     * opportunity duration.
     *
     * @param event the published ArbitrageOpportunity with its final status
     */
    public void recordEvent(ArbitrageOpportunity event) {
        String statusTag = event.getStatus().name().toLowerCase();

        Counter.builder(METRIC_OPPORTUNITY_EVENTS)
                .description("Arbitrage opportunity lifecycle events published to Kafka")
                .tag(TAG_STATUS, statusTag)
                .register(meterRegistry)
                .increment();

        if (event.getStatus() == OpportunityStatus.DETECTED) {
            DistributionSummary.builder(METRIC_NET_SPREAD_BPS)
                    .description("Net spread in basis points at detection time")
                    .register(meterRegistry)
                    .record(event.getNetSpreadBps().doubleValue());
        }

        if (event.getStatus() == OpportunityStatus.CLOSED || event.getStatus() == OpportunityStatus.EXPIRED) {
            DistributionSummary.builder(METRIC_OPPORTUNITY_DURATION_MS)
                    .description("Opportunity lifetime from DETECTED to CLOSED or EXPIRED (milliseconds)")
                    .tag(TAG_STATUS, statusTag)
                    .register(meterRegistry)
                    .record(event.getTotalDurationMs());
        }
    }

    /**
     * Records the time from detection consumer receive (T6) to comparison complete (T8).
     *
     * <p>This covers: Redis HGETALL (one per exchange per pair) + BigDecimal comparison
     * arithmetic + staleness gate. Split by exchange so we can identify whether one
     * exchange's feed systematically delays comparison (e.g., due to Redis layout).
     *
     * @param exchangeId   the exchange whose tick triggered the comparison
     * @param durationNanos T8 - T6 in nanoseconds
     */
    public void recordComparisonLatency(ExchangeId exchangeId, long durationNanos) {
        Timer.builder(METRIC_COMPARISON_LATENCY)
                .description("Time from detection consumer receive (T6) to comparison complete (T8)")
                .tag(TAG_EXCHANGE, exchangeId.name().toLowerCase())
                .register(meterRegistry)
                .record(Duration.ofNanos(durationNanos));
    }
}
