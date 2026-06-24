package com.arbitrage.simulator.model;

import com.arbitrage.common.model.ExchangeId;
import lombok.Builder;
import lombok.Value;

/**
 * Immutable latency model for a single exchange execution leg.
 *
 * <p>Represents the four components of time consumed between sending an order and
 * receiving its fill confirmation. All values are in milliseconds.
 *
 * <p><b>Why four components?</b> Decomposing latency makes it easier to reason about
 * which factor dominates — and to tune independently. Network latency is reduced by
 * co-locating with the exchange. Exchange processing latency is irreducible (it is the
 * exchange's matching engine speed). Confirmation latency mirrors network latency.
 * Jitter is the p99 overhead that accounts for variance — using p50 network figures
 * would underestimate total time in 50% of executions.
 *
 * <p><b>Default profiles (realistic estimates for non-co-located connections):</b>
 * <ul>
 *   <li>Binance: 15ms network + 5ms processing + 5ms confirmation = 25ms base + up to 15ms jitter</li>
 *   <li>Bybit:   25ms network + 8ms processing + 8ms confirmation = 41ms base + up to 20ms jitter</li>
 *   <li>KuCoin:  30ms network + 10ms processing + 10ms confirmation = 50ms base + up to 20ms jitter</li>
 * </ul>
 *
 * <p>A co-located HFT system targeting sub-millisecond execution would have entirely
 * different figures (network &lt;0.1ms, processing ~0.5ms). These defaults model a
 * realistic retail/institutional participant, not a top-tier HFT firm.
 */
@Value
@Builder
public class ExchangeLatencyProfile {

    /** The exchange this profile applies to. */
    ExchangeId exchangeId;

    /**
     * One-way outbound network latency in milliseconds — time for the order message
     * to travel from our system to the exchange's order entry gateway.
     */
    long networkLatencyMs;

    /**
     * Exchange-side order processing time in milliseconds — time the exchange's
     * matching engine takes to validate, queue, and match the incoming order.
     * This is irreducible from our side; it is the exchange's matching engine speed.
     */
    long exchangeProcessingMs;

    /**
     * One-way inbound confirmation latency in milliseconds — time for the fill
     * confirmation to travel back from the exchange to our system.
     * Typically mirrors {@link #networkLatencyMs} for symmetric connections.
     */
    long confirmationLatencyMs;

    /**
     * Maximum random jitter in milliseconds applied per simulation call.
     *
     * <p>In production, network latency is not a fixed constant — it varies with
     * routing changes, exchange load, and time of day. This field represents the
     * maximum additional latency that could be added above the deterministic base.
     * {@link com.arbitrage.simulator.service.ExecutionTimelineSimulator} samples
     * a uniformly random value in {@code [0, jitterMs]} independently for each leg
     * of every simulated trade. This produces realistic scatter on the Latency vs
     * Profitability chart instead of two fixed vertical lines.
     *
     * <p>Set to {@code 0} in tests to keep simulations deterministic.
     */
    long jitterMs;

    /**
     * Returns the deterministic base latency for this leg in milliseconds, excluding jitter.
     *
     * <p>Sum of the three fixed components: outbound network + exchange processing +
     * inbound confirmation. Jitter is intentionally excluded — it is sampled randomly
     * per simulation call inside
     * {@link com.arbitrage.simulator.service.ExecutionTimelineSimulator}.
     *
     * <p>Use this in tests where deterministic output is required. Production code
     * should call {@link com.arbitrage.simulator.service.ExecutionTimelineSimulator}
     * which adds the random jitter component.
     *
     * @return deterministic base leg latency in milliseconds (no jitter)
     */
    public long totalLegLatencyMs() {
        return networkLatencyMs + exchangeProcessingMs + confirmationLatencyMs;
    }
}
