-- ============================================================
-- Test-only schema: simulation_results as a plain PostgreSQL table.
--
-- Production uses TimescaleDB (timescale/timescaledb image) with
-- create_hypertable() and add_compression_policy() calls (Flyway V2+V4
-- in dashboard-api). Integration tests use plain postgres:alpine —
-- the TimescaleDB-specific functions are not available, and are not
-- needed to test the simulation pipeline's INSERT behaviour.
--
-- The column set and constraints are identical to V2 in production.
-- JPA entity validates against this schema identically.
-- ============================================================

CREATE TABLE IF NOT EXISTS simulation_results (
    id                          UUID            NOT NULL,
    opportunity_id              UUID            NOT NULL,
    simulated_latency_ms        BIGINT          NOT NULL,
    slippage_bps                NUMERIC(12,4)   NOT NULL DEFAULT 0,
    simulated_buy_price         NUMERIC(20,8)   NOT NULL,
    simulated_sell_price        NUMERIC(20,8)   NOT NULL,
    simulated_quantity          NUMERIC(20,8)   NOT NULL,
    was_profitable              BOOLEAN         NOT NULL,
    net_profit                  NUMERIC(20,8)   NOT NULL,
    fill_probability            NUMERIC(5,4)    NOT NULL DEFAULT 1.0,
    simulation_timestamp        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_simulations PRIMARY KEY (id, simulation_timestamp),
    CONSTRAINT chk_fill_probability CHECK (fill_probability >= 0 AND fill_probability <= 1),
    CONSTRAINT chk_positive_sim_quantity CHECK (simulated_quantity > 0),
    CONSTRAINT chk_positive_latency CHECK (simulated_latency_ms >= 0)
);
