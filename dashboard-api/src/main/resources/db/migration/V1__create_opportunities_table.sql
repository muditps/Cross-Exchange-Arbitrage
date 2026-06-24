-- ============================================================
-- V1: Create arbitrage_opportunities table
-- ============================================================
-- Stores every detected arbitrage opportunity with its full lifecycle.
--
-- WHY THIS TABLE EXISTS:
-- When the detection engine finds a price discrepancy across exchanges
-- that exceeds transaction fees, it creates a record here. As the
-- opportunity evolves (spread widens/narrows), the record is updated.
-- When the spread disappears, the opportunity is closed.
--
-- WHY NUMERIC(20,8):
-- PostgreSQL's NUMERIC is arbitrary-precision decimal — the database
-- equivalent of Java's BigDecimal. NUMERIC(20,8) means 20 total digits,
-- 8 after the decimal. Handles prices up to 999,999,999,999.99999999.
-- NEVER use FLOAT or DOUBLE PRECISION for financial data — same
-- IEEE 754 rounding problems as Java's double.
--
-- WHY TIMESTAMPTZ:
-- TIMESTAMPTZ stores timestamps with timezone information. Crypto markets
-- run 24/7 globally. Without timezone, a timestamp of "2026-03-22 14:00:00"
-- is ambiguous — is that UTC? IST? EST? TIMESTAMPTZ removes ambiguity.
--
-- WHY UUID:
-- UUIDs are globally unique without a central counter. In a distributed
-- system, two detection engine instances can create opportunities
-- simultaneously without ID collisions. Auto-increment IDs would require
-- coordination or risk duplicates.
-- ============================================================

CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    -- Primary identifier
    -- Composite primary key: (id, detection_timestamp)
    -- WHY COMPOSITE: TimescaleDB partitions data by detection_timestamp.
    -- Unique indexes on partitioned tables MUST include the partition column.
    -- A standalone PRIMARY KEY (id) would require scanning ALL partitions to
    -- verify uniqueness — defeating the purpose of partitioning.
    id                      UUID            NOT NULL DEFAULT gen_random_uuid(),

    -- Trading pair (e.g., "BTC-USDT", "RELIANCE-INR")
    trading_pair            VARCHAR(20)     NOT NULL,

    -- Which exchanges are involved
    buy_exchange            VARCHAR(20)     NOT NULL,
    sell_exchange           VARCHAR(20)     NOT NULL,

    -- Prices at detection time
    -- buy_price = ask price on buy exchange (what we'd pay)
    -- sell_price = bid price on sell exchange (what we'd receive)
    buy_price               NUMERIC(20,8)  NOT NULL,
    sell_price              NUMERIC(20,8)  NOT NULL,

    -- Spread calculations (in basis points, 1 bps = 0.01%)
    -- raw_spread_bps = spread before fees
    -- net_spread_bps = spread after deducting both exchange taker fees
    raw_spread_bps          NUMERIC(12,4)  NOT NULL,
    net_spread_bps          NUMERIC(12,4)  NOT NULL,

    -- Fee rates at time of detection (stored for auditability)
    buy_fee_rate            NUMERIC(10,6)  NOT NULL,
    sell_fee_rate           NUMERIC(10,6)  NOT NULL,

    -- Quantity and estimated profit
    -- quantity = min(buy_ask_quantity, sell_bid_quantity)
    -- estimated_profit = net_spread * quantity (approximate)
    quantity                NUMERIC(20,8)  NOT NULL,
    estimated_profit        NUMERIC(20,8)  NOT NULL,

    -- Opportunity lifecycle status
    -- DETECTED → OPEN → CLOSED or EXPIRED
    status                  VARCHAR(20)    NOT NULL DEFAULT 'DETECTED',

    -- Timestamps
    -- detection_timestamp: when first detected (partition column for hypertable)
    -- last_updated_timestamp: last price/status update
    -- closed_timestamp: when opportunity ended (NULL if still open)
    detection_timestamp     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    last_updated_timestamp  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    closed_timestamp        TIMESTAMPTZ,

    -- Analytics fields (updated during opportunity lifecycle)
    -- peak_net_spread_bps: highest spread observed during this opportunity
    -- average_net_spread_bps: running average of spread observations
    -- total_duration_ms: total time from detection to close (NULL if still open)
    peak_net_spread_bps     NUMERIC(12,4),
    average_net_spread_bps  NUMERIC(12,4),
    total_duration_ms       BIGINT,

    -- Constraints
    CONSTRAINT pk_opportunities PRIMARY KEY (id, detection_timestamp),
    CONSTRAINT chk_status CHECK (status IN ('DETECTED', 'OPEN', 'CLOSED', 'EXPIRED')),
    CONSTRAINT chk_positive_quantity CHECK (quantity > 0),
    CONSTRAINT chk_different_exchanges CHECK (buy_exchange <> sell_exchange)
);

-- ============================================================
-- Convert to TimescaleDB hypertable
-- ============================================================
-- A hypertable automatically partitions data by time into chunks.
-- When you query "opportunities in the last hour," TimescaleDB only
-- scans the relevant chunk — not the entire table.
--
-- chunk_time_interval = 1 day: each chunk holds one day of data.
-- For our expected volume (~1000 opportunities/day), daily chunks
-- provide good query performance without too many small chunks.
-- ============================================================

SELECT create_hypertable(
    'arbitrage_opportunities',
    'detection_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
