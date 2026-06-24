package com.arbitrage.dashboard.opportunity;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.util.KafkaHeaderUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;

/**
 * Consumes {@link ArbitrageOpportunity} lifecycle events from Kafka and upserts
 * them into the {@code arbitrage_opportunities} TimescaleDB table.
 *
 * <p>This is the persistence bridge between the detection engine (which publishes
 * opportunities to Kafka) and the REST/analytics queries (which read from TimescaleDB).
 * Without this listener, the {@code arbitrage_opportunities} table is never written
 * and all REST endpoints return empty results.
 *
 * <p><b>Consumer group:</b> {@code dashboard-opportunity-persistence-group} — independent
 * from the WebSocket broadcaster group and the execution simulator group. Each group
 * receives every event independently; this one owns DB persistence.
 *
 * <p><b>All four statuses are upserted:</b> DETECTED creates the initial row; OPEN, CLOSED,
 * and EXPIRED update the mutable lifecycle fields (status, closed_timestamp, peak spread,
 * duration). The INSERT ON CONFLICT DO UPDATE pattern is safe to replay — if a message is
 * redelivered, the upsert is idempotent.
 *
 * <p><b>Why JdbcTemplate here and not in a Schedulers.boundedElastic()?</b>
 * Kafka listeners run on Spring Kafka's consumer thread pool — not the Netty I/O threads.
 * Blocking JDBC calls are safe from a listener method. Wrapping in boundedElastic() is
 * only needed when calling from inside a reactive pipeline (e.g., a WebFlux endpoint).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OpportunityPersistenceListener {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private static final String UPSERT_SQL = """
            INSERT INTO arbitrage_opportunities (
                id, trading_pair, buy_exchange, sell_exchange,
                buy_price, sell_price, raw_spread_bps, net_spread_bps,
                buy_fee_rate, sell_fee_rate, quantity, estimated_profit,
                status, detection_timestamp, last_updated_timestamp, closed_timestamp,
                peak_net_spread_bps, average_net_spread_bps, total_duration_ms
            ) VALUES (
                ?::uuid, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?
            )
            ON CONFLICT (id, detection_timestamp) DO UPDATE SET
                status                 = EXCLUDED.status,
                last_updated_timestamp = EXCLUDED.last_updated_timestamp,
                closed_timestamp       = EXCLUDED.closed_timestamp,
                peak_net_spread_bps    = EXCLUDED.peak_net_spread_bps,
                average_net_spread_bps = EXCLUDED.average_net_spread_bps,
                total_duration_ms      = EXCLUDED.total_duration_ms
            """;

    /**
     * Receives an opportunity lifecycle event and upserts it to TimescaleDB.
     *
     * <p>After persisting, reads T0–T9 latency headers from the Kafka record and logs
     * a pipeline breakdown at INFO level for DETECTED events. Segments:
     * <ul>
     *   <li>T2→T3: connector→normaliser Kafka transit</li>
     *   <li>T5→T6: normaliser→detector Kafka transit</li>
     *   <li>T6→T8: detection processing (Redis + comparison)</li>
     *   <li>T8→T9: detection→opportunity publish overhead</li>
     *   <li>T1→T9: full parse-to-publish pipeline latency</li>
     * </ul>
     *
     * <p>T1–T9 are {@link System#nanoTime()} values. On Linux (Docker deployment), nanoTime
     * uses {@code CLOCK_MONOTONIC} which is shared across processes on the same host, making
     * cross-JVM latency segments valid. Values are 0 for messages produced before
     * this instrumentation was deployed (backward-compatible via {@link KafkaHeaderUtils#read}).
     *
     * @param record the raw Kafka record containing opportunity JSON and latency headers
     */
    @KafkaListener(
            topics = "arbitrage-opportunities",
            groupId = "dashboard-opportunity-persistence-group"
    )
    public void onOpportunity(final ConsumerRecord<String, String> record) {
        try {
            final ArbitrageOpportunity opp = objectMapper.readValue(record.value(), ArbitrageOpportunity.class);
            final boolean isClosed = opp.getStatus() == OpportunityStatus.CLOSED
                                  || opp.getStatus() == OpportunityStatus.EXPIRED;

            jdbcTemplate.update(UPSERT_SQL,
                    opp.getId().toString(),
                    opp.getTradingPair().canonicalSymbol(),
                    opp.getBuyExchange().name(),
                    opp.getSellExchange().name(),
                    opp.getBuyPrice(),
                    opp.getSellPrice(),
                    opp.getGrossSpreadBps(),
                    opp.getNetSpreadBps(),
                    opp.getBuyFeeRate(),
                    opp.getSellFeeRate(),
                    opp.getArbitrageableQuantity(),
                    opp.getTheoreticalProfit(),
                    opp.getStatus().name(),
                    Timestamp.from(opp.getDetectionTimestamp()),
                    Timestamp.from(opp.getLastUpdateTimestamp()),
                    isClosed ? Timestamp.from(opp.getLastUpdateTimestamp()) : null,
                    toBps(opp.getPeakNetSpread(), opp.getBuyPrice(), opp.getNetSpreadBps()),
                    toBps(opp.getAverageNetSpread(), opp.getBuyPrice(), opp.getNetSpreadBps()),
                    opp.getTotalDurationMs()
            );
            log.debug("Persisted opportunity: id={} status={} pair={}",
                    opp.getId(), opp.getStatus(), opp.getTradingPair().canonicalSymbol());

            logPipelineLatency(record, opp);
        } catch (Exception e) {
            log.error("Failed to persist opportunity: partition={} offset={} error={}",
                    record.partition(), record.offset(), e.getMessage(), e);
        }
    }

    /**
     * Reads T1–T9 latency headers and logs a structured pipeline breakdown.
     * Only logs for DETECTED events to avoid log noise on CLOSED/EXPIRED updates.
     * Skips logging when T1 is 0 (pre-instrumentation messages or EXPIRED events).
     */
    private void logPipelineLatency(final ConsumerRecord<String, String> record,
                                    final ArbitrageOpportunity opp) {
        if (opp.getStatus() != OpportunityStatus.DETECTED) {
            return;
        }
        final long t1 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T1);
        if (t1 == 0L) {
            return; // headers absent — pre-instrumentation message or EXPIRED event
        }
        final long t2 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T2);
        final long t3 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T3);
        final long t5 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T5);
        final long t6 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T6);
        final long t8 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T8);
        final long t9 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T9);

        log.info("Pipeline latency: pair={} buy={} sell={} " +
                        "connectorToNormaliserKafkaMs={} " +
                        "normaliserToDetectorKafkaMs={} " +
                        "detectionProcessingMs={} " +
                        "publishOverheadUs={} " +
                        "endToEndMs={}",
                opp.getTradingPair().canonicalSymbol(),
                opp.getBuyExchange(), opp.getSellExchange(),
                (t3 - t2) / 1_000_000.0,
                (t6 - t5) / 1_000_000.0,
                (t8 - t6) / 1_000_000.0,
                (t9 - t8) / 1_000.0,
                (t9 - t1) / 1_000_000.0);
    }

    /**
     * Converts an absolute spread value to basis points using the buy price as the divisor.
     * Falls back to {@code fallbackBps} when {@code spread} is null or zero (e.g., on first
     * DETECTED event before tracking fields are populated by the OpportunityTracker).
     *
     * @param spread      absolute spread (e.g., peakNetSpread in USD)
     * @param buyPrice    reference price for the bps conversion
     * @param fallbackBps bps value to use when spread is absent
     * @return spread in basis points
     */
    private static BigDecimal toBps(final BigDecimal spread,
                                    final BigDecimal buyPrice,
                                    final BigDecimal fallbackBps) {
        if (spread == null || spread.compareTo(BigDecimal.ZERO) == 0 || buyPrice == null
                || buyPrice.compareTo(BigDecimal.ZERO) == 0) {
            return fallbackBps;
        }
        return spread.divide(buyPrice, 12, RoundingMode.HALF_UP)
                     .multiply(BigDecimal.valueOf(10_000))
                     .setScale(4, RoundingMode.HALF_UP);
    }
}
