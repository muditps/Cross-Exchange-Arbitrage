package com.arbitrage.simulator.repository;

import com.arbitrage.simulator.entity.SimulationResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Spring Data JPA repository for {@link SimulationResult} persistence.
 *
 * <p>The execution simulator is a write-only producer of simulation records — it INSERTs
 * one row per closed opportunity and never reads, updates, or deletes existing rows.
 * The dashboard API owns all read queries (REST endpoints for simulation history,
 * profitability analysis, latency distribution). Those will be added in Phase 5.
 *
 * <p>The inherited {@link JpaRepository#save(Object)} method is the only operation
 * the simulator uses. Spring Data JPA handles the INSERT statement, including the
 * {@code simulation_timestamp} used by TimescaleDB as the hypertable partition key.
 *
 * <p><b>Why no custom queries here?</b> All dashboard read queries go through
 * {@code dashboard-api}, not through this module. Keeping queries out of this
 * repository prevents the simulator from accumulating dashboard concerns.
 */
@Repository
public interface SimulationResultRepository extends JpaRepository<SimulationResult, UUID> {
}
