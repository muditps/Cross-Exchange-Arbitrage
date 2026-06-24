package com.arbitrage.simulator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity representing one execution simulation outcome for a closed arbitrage opportunity.
 *
 * <p>Maps to the {@code simulation_results} TimescaleDB hypertable created by Flyway V2.
 * Each row answers: "If we had traded this opportunity, what would the real P&amp;L have been?"
 *
 * <p><b>Composite PK note:</b> The database defines {@code PRIMARY KEY (id, simulation_timestamp)}
 * because TimescaleDB hypertables require the partition column in the primary key. JPA maps
 * only {@code id} as the {@code @Id} — functionally correct for a write-only entity. The
 * simulator only INSERTs records; it never UPDATEs or DELETEs by primary key.
 *
 * <p><b>UUID pre-generation:</b> The {@code id} is generated in Java via
 * {@code UUID.randomUUID()} rather than relying on the database {@code DEFAULT gen_random_uuid()}.
 * This gives us the ID before the INSERT, allowing the {@code SimulationOrchestrator} to log
 * the record ID immediately without a database round-trip.
 *
 * <p><b>Fill probability = 1.0:</b> This session assumes full fills (all available quantity
 * is executed). Partial fill modelling requires order book depth snapshots not currently
 * captured. This field is persisted for future use when depth data is available (Phase 6+).
 *
 * <p><b>ddl-auto=validate:</b> This module does not run Flyway migrations — that is
 * {@code dashboard-api}'s responsibility. Hibernate validates the entity against the
 * existing schema on startup. Precision/scale annotations on {@code @Column} are for
 * documentation only — they are not used when {@code ddl-auto=validate}.
 */
@Entity
@Table(name = "simulation_results")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SimulationResult {

    /** Unique identifier for this simulation record. Pre-generated in Java. */
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /**
     * The opportunity this simulation is based on.
     * Links back to the {@code arbitrage_opportunities} table (created by Flyway V1).
     */
    @Column(name = "opportunity_id", nullable = false, updatable = false)
    private UUID opportunityId;

    /**
     * Total simulated execution time in milliseconds:
     * {@code detectionToDecisionMs + max(buyLegLatencyMs, sellLegLatencyMs)}.
     * Computed by {@link com.arbitrage.simulator.service.ExecutionTimelineSimulator}.
     */
    @Column(name = "simulated_latency_ms", nullable = false)
    private long simulatedLatencyMs;

    /**
     * Combined slippage across both legs in basis points.
     * Positive = adverse. Computed by {@link com.arbitrage.simulator.service.SlippageEstimator}.
     */
    @Column(name = "slippage_bps", nullable = false, precision = 12, scale = 4)
    private BigDecimal slippageBps;

    /**
     * The simulated fill price on the buy exchange — the ask price at
     * {@code detectionTimestamp + simulatedLatencyMs}.
     * This is what we actually paid, as opposed to the detection-time ask price.
     */
    @Column(name = "simulated_buy_price", nullable = false, precision = 20, scale = 8)
    private BigDecimal simulatedBuyPrice;

    /**
     * The simulated fill price on the sell exchange — the bid price at
     * {@code detectionTimestamp + simulatedLatencyMs}.
     * This is what we actually received, as opposed to the detection-time bid price.
     */
    @Column(name = "simulated_sell_price", nullable = false, precision = 20, scale = 8)
    private BigDecimal simulatedSellPrice;

    /**
     * Quantity simulated: {@code min(buyQuantity, sellQuantity)} from the opportunity.
     * Bounded by the thinner side of the order book at detection time.
     */
    @Column(name = "simulated_quantity", nullable = false, precision = 20, scale = 8)
    private BigDecimal simulatedQuantity;

    /**
     * Whether the simulated trade was profitable after fees and slippage.
     * {@code true} when {@link #netProfit} {@code > 0}.
     */
    @Column(name = "was_profitable", nullable = false)
    private boolean wasProfitable;

    /**
     * Net profit after fees and slippage in the quote currency (e.g., USDT).
     *
     * <p>Formula:
     * {@code (simulatedSellPrice − simulatedBuyPrice) × quantity
     *        − (simulatedBuyPrice × buyFeeRate × quantity)
     *        − (simulatedSellPrice × sellFeeRate × quantity)}.
     * Can be negative if slippage erodes the spread entirely.
     */
    @Column(name = "net_profit", nullable = false, precision = 20, scale = 8)
    private BigDecimal netProfit;

    /**
     * Estimated probability that both legs fill completely.
     * Always {@code 1.0} in this session — full fills are assumed.
     * Partial fill modelling is deferred to Phase 6 when order book depth data is available.
     */
    @Column(name = "fill_probability", nullable = false, precision = 5, scale = 4)
    private BigDecimal fillProbability;

    /**
     * Wall-clock instant when this simulation was computed.
     * Used as the TimescaleDB partition key — all time-range queries on this table
     * use this column (e.g., "show simulations from the last 24 hours").
     */
    @Column(name = "simulation_timestamp", nullable = false)
    private Instant simulationTimestamp;
}
