-- ============================================================
-- V4: TimescaleDB compression policies
-- ============================================================
-- Compresses chunks older than 7 days to save storage.
--
-- WHY COMPRESSION:
-- After 7 days, opportunity data is rarely accessed in detail.
-- TimescaleDB compression reduces storage by ~90% by converting
-- row-oriented data to columnar format within each chunk.
--
-- TRADEOFF:
-- Compressed data is read-only (inserts and updates fail on
-- compressed chunks). Since opportunities older than 7 days are
-- historical and never updated, this is perfectly safe.
-- Queries on compressed data are slightly slower (~2-3x) but
-- still fast because TimescaleDB decompresses only the needed
-- columns, not the entire chunk.
--
-- KEY CONCEPT — Columnar vs Row Storage:
-- Row storage: each row is stored contiguously (fast for inserts)
-- Columnar storage: each column is stored contiguously (great compression,
-- fast for analytics like "average spread across all opportunities")
-- TimescaleDB compression converts from row to columnar — perfect for
-- our use case where old data is only read for analytics, never modified.
-- ============================================================

-- Enable compression on the opportunities table
-- segment_by: group compressed data by trading_pair (most common filter)
-- order_by: sort compressed data by detection_timestamp DESC (most common sort)
ALTER TABLE arbitrage_opportunities SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'trading_pair',
    timescaledb.compress_orderby = 'detection_timestamp DESC'
);

-- Automatically compress chunks older than 7 days
SELECT add_compression_policy(
    'arbitrage_opportunities',
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression on simulation results
ALTER TABLE simulation_results SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'opportunity_id',
    timescaledb.compress_orderby = 'simulation_timestamp DESC'
);

-- Automatically compress simulation chunks older than 7 days
SELECT add_compression_policy(
    'simulation_results',
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE
);
