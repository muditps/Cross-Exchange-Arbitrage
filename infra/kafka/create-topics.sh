#!/bin/bash
# ============================================================
# Kafka Topic Initialisation Script
# ============================================================
# Creates all required Kafka topics with appropriate configurations.
# Runs once via the kafka-init container on infrastructure startup.
#
# Topic naming convention: kebab-case
# Retention strategy:
#   - Raw ticks (24h): High-volume, ephemeral. We can re-receive
#     from the exchange if needed. Keeping longer wastes storage.
#   - Normalised ticks (7 days): Medium volume, useful for replay
#     testing and debugging normalisation issues.
#   - Opportunities (30 days): Low volume, analytically valuable.
#     Feeds dashboard charts, backtesting, and daily summaries.
#   - Latency metrics (7 days): Medium volume, used for performance
#     analysis and bottleneck identification.
# ============================================================

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="kafka:9092"

echo "============================================"
echo "  Kafka Topic Initialisation"
echo "============================================"

# Wait for Kafka to be fully ready (healthcheck may pass before
# the broker is ready to create topics)
echo "Waiting for Kafka broker to be ready..."
sleep 5

# ---- Helper function ----
# Creates a topic if it doesn't already exist.
# Parameters: topic_name, partitions, replication_factor, retention_ms
create_topic() {
    local topic=$1
    local partitions=$2
    local replication=$3
    local retention_ms=$4
    local retention_display=$5

    echo ""
    echo "Creating topic: ${topic}"
    echo "  Partitions: ${partitions}, Replication: ${replication}, Retention: ${retention_display}"

    ${KAFKA_BIN}/kafka-topics.sh \
        --bootstrap-server ${BOOTSTRAP} \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${replication}" \
        --config retention.ms="${retention_ms}"

    if [ $? -eq 0 ]; then
        echo "  ✓ ${topic} ready"
    else
        echo "  ✗ Failed to create ${topic}"
    fi
}

# ---- Raw tick topics (one per exchange) ----
# Retention: 24 hours (86400000 ms)
# These carry the raw WebSocket messages exactly as received from
# each exchange, before any normalisation. High volume (~100 msgs/sec
# per exchange), but ephemeral — we can always re-receive from the
# exchange's live feed.
RETENTION_24H=86400000

create_topic "raw-ticks-binance"  1  1  ${RETENTION_24H}  "24 hours"
create_topic "raw-ticks-bybit"    1  1  ${RETENTION_24H}  "24 hours"
create_topic "raw-ticks-kucoin"   1  1  ${RETENTION_24H}  "24 hours"

# ---- Normalised tick topic ----
# Retention: 7 days (604800000 ms)
# Unified format ticks from all exchanges. Used by the detection
# engine and useful for replay testing (replaying a week of ticks
# to test detection logic changes without live exchange connections).
RETENTION_7D=604800000

create_topic "normalised-ticks"  3  1  ${RETENTION_7D}  "7 days"

# ---- Arbitrage opportunity topic ----
# Retention: 30 days (2592000000 ms)
# Lifecycle events: DETECTED, CLOSED, EXPIRED. Low volume (maybe
# a few per hour on major pairs). Analytically valuable — feeds
# the dashboard's historical charts and daily summary reports.
RETENTION_30D=2592000000

create_topic "arbitrage-opportunities"  3  1  ${RETENTION_30D}  "30 days"

# ---- Latency metrics topic ----
# Retention: 7 days
# Pipeline latency measurements (T0-T9 timestamps). Used by the
# performance monitoring system and the latency breakdown waterfall
# chart on the dashboard.
create_topic "metrics-latency"  1  1  ${RETENTION_7D}  "7 days"

# ---- List all topics ----
echo ""
echo "============================================"
echo "  All topics:"
echo "============================================"
${KAFKA_BIN}/kafka-topics.sh \
    --bootstrap-server ${BOOTSTRAP} \
    --list

echo ""
echo "Topic initialisation complete."
