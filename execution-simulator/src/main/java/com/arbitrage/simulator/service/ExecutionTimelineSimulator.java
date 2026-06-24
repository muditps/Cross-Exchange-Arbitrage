package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.simulator.config.LatencyConfiguration;
import com.arbitrage.simulator.model.ExchangeLatencyProfile;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates the total execution time for a two-legged arbitrage trade.
 *
 * <p><b>Execution model (pre-funded):</b> Both buy and sell orders are dispatched
 * simultaneously the moment an arbitrage decision is made. The total wall-clock time is:
 * <pre>
 *   totalMs = detectionToDecisionMs + max(buyLegTotalMs, sellLegTotalMs)
 * </pre>
 *
 * <p><b>Why {@code max()} not {@code sum()}?</b>
 * Both legs execute in parallel — wall-clock time is bounded by the slower leg, not
 * the combined time. Summing would model serial execution, overstating latency by
 * up to 2× and making viable opportunities appear uncapturable.
 *
 * <p><b>Why add {@code detectionToDecisionMs} separately?</b>
 * This is the serial portion: the detection engine must finish computing before orders
 * can be dispatched. It cannot overlap with network transit. It is strictly additive.
 *
 * <p><b>Why each leg includes four components?</b>
 * Network (outbound) + exchange processing + confirmation (inbound) + jitter.
 * Decomposing latency makes it clear which factor is reducible (network → co-location)
 * vs irreducible (exchange processing → exchange matching engine speed).
 *
 * <p>This simulator is deterministic — identical inputs always produce identical output.
 * It does not model probabilistic fill rates, queue position, or order book depth.
 */
@Component
@RequiredArgsConstructor
public class ExecutionTimelineSimulator {

    private final LatencyConfiguration latencyConfiguration;

    /**
     * Returns the simulated total execution time in milliseconds for a two-legged trade.
     *
     * <p>Formula: {@code detectionToDecisionMs + max(buyLegTotal, sellLegTotal)},
     * where each leg total = networkLatencyMs + exchangeProcessingMs
     *                      + confirmationLatencyMs + jitterMs.
     *
     * @param buyExchange           exchange where the buy order is placed
     * @param sellExchange          exchange where the sell order is placed
     * @param detectionToDecisionMs serial time from opportunity detection to order dispatch
     * @return simulated total execution time in milliseconds
     */
    public long simulateExecutionTimeMs(ExchangeId buyExchange,
                                        ExchangeId sellExchange,
                                        long detectionToDecisionMs) {
        ExchangeLatencyProfile buyProfile  = latencyConfiguration.getProfile(buyExchange);
        ExchangeLatencyProfile sellProfile = latencyConfiguration.getProfile(sellExchange);
        long maxLegMs = Math.max(sampleLegLatencyMs(buyProfile), sampleLegLatencyMs(sellProfile));
        return detectionToDecisionMs + maxLegMs;
    }

    /**
     * Samples the total latency for one execution leg by adding a random jitter
     * component to the deterministic base latency.
     *
     * <p>Each leg is sampled independently, so buy and sell legs get different jitter
     * values — matching reality where routing to two different exchanges varies
     * independently. The random value is drawn uniformly from {@code [0, jitterMs]},
     * so the deterministic base is always the floor (never faster than the model predicts)
     * and the maximum is {@code base + jitterMs}.
     *
     * <p>When {@code jitterMs == 0} (used in tests), the result is fully deterministic.
     *
     * @param profile the exchange latency profile to sample
     * @return sampled total leg latency in milliseconds
     */
    private long sampleLegLatencyMs(final ExchangeLatencyProfile profile) {
        long base = profile.totalLegLatencyMs();
        long maxJitter = profile.getJitterMs();
        long sampledJitter = maxJitter > 0
                ? ThreadLocalRandom.current().nextLong(maxJitter + 1)
                : 0L;
        return base + sampledJitter;
    }
}
