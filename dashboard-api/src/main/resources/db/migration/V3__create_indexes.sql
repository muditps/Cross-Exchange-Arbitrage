-- ============================================================
-- V3: Create indexes for common query patterns
-- ============================================================
-- Without indexes, every query scans the entire table (full table scan).
-- With indexes, PostgreSQL jumps directly to matching rows.
--
-- WHY THESE SPECIFIC INDEXES:
-- We create indexes based on how the dashboard will query data:
-- 1. Filter by trading pair ("show me BTC-USDT opportunities")
-- 2. Filter by status ("show me OPEN opportunities")
-- 3. Filter by exchange ("show me Binance opportunities")
-- 4. Composite: pair + time ("BTC-USDT opportunities last hour")
-- 5. Link simulations to opportunities
--
-- KEY CONCEPT — Index Tradeoffs:
-- Indexes speed up reads but slow down writes (every INSERT must
-- update the index too). For our workload (~1000 opportunities/day,
-- dashboard reads every 100ms), the read benefit far outweighs
-- the write cost.
--
-- KEY CONCEPT — Composite Indexes:
-- An index on (trading_pair, detection_timestamp) is different from
-- two separate indexes. The composite index handles "WHERE trading_pair
-- = 'BTC-USDT' AND detection_timestamp > '2026-03-22'" in a single
-- index lookup. PostgreSQL reads the index left-to-right, so the
-- column order matters — put the most selective (filtering) column first.
-- ============================================================

-- === Opportunities Table Indexes ===

-- Filter by trading pair (most common dashboard filter)
CREATE INDEX IF NOT EXISTS idx_opportunities_trading_pair
    ON arbitrage_opportunities (trading_pair);

-- Filter by status (e.g., "show all OPEN opportunities")
CREATE INDEX IF NOT EXISTS idx_opportunities_status
    ON arbitrage_opportunities (status);

-- Filter by exchange (e.g., "opportunities involving Binance")
CREATE INDEX IF NOT EXISTS idx_opportunities_buy_exchange
    ON arbitrage_opportunities (buy_exchange);

CREATE INDEX IF NOT EXISTS idx_opportunities_sell_exchange
    ON arbitrage_opportunities (sell_exchange);

-- Composite: trading pair + detection time
-- Covers the most common dashboard query: "BTC-USDT opportunities in the last hour"
-- The hypertable already partitions by detection_timestamp, but this index
-- speeds up queries within a single time chunk.
CREATE INDEX IF NOT EXISTS idx_opportunities_pair_time
    ON arbitrage_opportunities (trading_pair, detection_timestamp DESC);

-- Composite: status + detection time
-- Covers "all OPEN opportunities sorted by newest first"
CREATE INDEX IF NOT EXISTS idx_opportunities_status_time
    ON arbitrage_opportunities (status, detection_timestamp DESC);

-- === Simulation Results Table Indexes ===

-- Link simulations to their parent opportunity
CREATE INDEX IF NOT EXISTS idx_simulations_opportunity_id
    ON simulation_results (opportunity_id);

-- Filter by profitability (e.g., "show only profitable simulations")
CREATE INDEX IF NOT EXISTS idx_simulations_profitable
    ON simulation_results (was_profitable, simulation_timestamp DESC);
