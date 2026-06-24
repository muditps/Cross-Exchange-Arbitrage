-- ============================================================
-- V2: Create simulation_results table
-- ============================================================
-- Stores execution simulation outcomes for detected opportunities.
--
-- WHY THIS TABLE EXISTS:
-- Detecting an opportunity is only half the story. Could you actually
-- CAPTURE it? The execution simulator models realistic conditions:
-- - Network latency (how long to send an order)
-- - Order book slippage (the price moves while your order travels)
-- - Fill probability (is there enough liquidity?)
-- - Net P&L after all costs
--
-- Each simulation result links back to an opportunity via opportunity_id.
-- One opportunity can have multiple simulations (e.g., with different
-- latency assumptions or slippage models).
-- ============================================================

CREATE TABLE IF NOT EXISTS simulation_results (
    -- Primary identifier (composite with partition column)
    id                          UUID            NOT NULL DEFAULT gen_random_uuid(),

    -- Link to the opportunity that was simulated
    opportunity_id              UUID            NOT NULL,

    -- Simulation parameters (inputs)
    -- simulated_latency_ms: assumed network round-trip time
    -- slippage_bps: assumed price movement during order transit
    simulated_latency_ms        BIGINT          NOT NULL,
    slippage_bps                NUMERIC(12,4)   NOT NULL DEFAULT 0,

    -- Simulated execution prices (may differ from detection prices due to slippage)
    simulated_buy_price         NUMERIC(20,8)   NOT NULL,
    simulated_sell_price        NUMERIC(20,8)   NOT NULL,

    -- Simulated quantity (may be less than detected if liquidity dried up)
    simulated_quantity          NUMERIC(20,8)   NOT NULL,

    -- Outcome
    -- was_profitable: did the simulated trade make money after all costs?
    -- net_profit: actual P&L after fees + slippage (can be negative)
    -- fill_probability: estimated chance of both orders being filled (0.0 to 1.0)
    was_profitable              BOOLEAN         NOT NULL,
    net_profit                  NUMERIC(20,8)   NOT NULL,
    fill_probability            NUMERIC(5,4)    NOT NULL DEFAULT 1.0,

    -- Timing
    simulation_timestamp        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT pk_simulations PRIMARY KEY (id, simulation_timestamp),
    CONSTRAINT chk_fill_probability CHECK (fill_probability >= 0 AND fill_probability <= 1),
    CONSTRAINT chk_positive_sim_quantity CHECK (simulated_quantity > 0),
    CONSTRAINT chk_positive_latency CHECK (simulated_latency_ms >= 0)
);

-- ============================================================
-- Convert to TimescaleDB hypertable
-- ============================================================
-- Partitioned by simulation_timestamp for efficient time-range queries.
-- "Show me all simulations from last week" scans only relevant chunks.
-- ============================================================

SELECT create_hypertable(
    'simulation_results',
    'simulation_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
