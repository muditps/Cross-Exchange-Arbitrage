package com.arbitrage.simulator.config;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/**
 * Configuration properties for the Execution Simulator.
 *
 * <p>All values are overrideable via environment variables — no recompile needed.
 * This matters in Phase 6 (latency profiling) where we run the same simulation
 * at different assumed latency budgets to produce the "latency vs profitability"
 * scatter chart.
 *
 * <p>Bound under the prefix {@code arbitrage.simulation}. Sessions 4.2–4.4
 * add per-exchange latency profiles and per-opportunity fee/slippage config;
 * this class holds the module-level defaults.
 */
@ConfigurationProperties(prefix = "arbitrage.simulation")
@Validated
@Getter
@Setter
public class SimulationProperties {

    /**
     * Default slippage in basis points applied to each simulated leg.
     *
     * <p>Slippage models the price movement between when an order is placed
     * and when it is filled. At 0.02% (2 bps) default, a $40,000 BTC order
     * loses ~$8 per leg to slippage. Session 4.4 ({@code SlippageEstimator})
     * uses this value as the baseline; Phase 7 may replace it with order-book
     * depth data for a more precise estimate.
     *
     * <p>Must be non-negative. Set to 0 for a fee-only (no-slippage) simulation.
     */
    @NotNull
    @DecimalMin(value = "0.0", inclusive = true, message = "defaultSlippageBps must be >= 0")
    private BigDecimal defaultSlippageBps = new BigDecimal("2.00");

    /**
     * Size of the historical price replay window in seconds.
     *
     * <p>The {@code HistoricalPriceStore} (Session 4.3) keeps a rolling buffer
     * of ticks for this duration per exchange-pair. Must cover the maximum
     * simulated execution latency with headroom: at 30ms max latency,
     * a 60-second window is more than sufficient.
     */
    @Min(value = 10, message = "historicalWindowSeconds must be at least 10")
    private int historicalWindowSeconds = 60;

    /**
     * Serial time in milliseconds from opportunity detection to order dispatch.
     *
     * <p>Represents internal processing latency: the time our system takes to
     * evaluate the opportunity and issue order commands after the detection engine
     * publishes the event. In a production co-located system this would be measured
     * precisely (typically &lt;1ms). For simulation we use a configurable constant.
     *
     * <p>This value is additive in the execution timeline formula:
     * {@code totalMs = detectionToDecisionMs + max(buyLeg, sellLeg)}.
     * It cannot overlap with network transit — the decision must complete before
     * orders can be dispatched.
     *
     * <p>Default: 5ms — a realistic estimate for a non-co-located participant
     * with a simple decision algorithm (no complex ML inference).
     */
    @Min(value = 0, message = "detectionToDecisionMs must be >= 0")
    private long detectionToDecisionMs = 5L;

    /**
     * Whether simulation is active. Set to {@code false} in test environments
     * where TimescaleDB is unavailable, to prevent the consumer from attempting
     * persistence and failing.
     */
    private boolean enabled = true;
}
