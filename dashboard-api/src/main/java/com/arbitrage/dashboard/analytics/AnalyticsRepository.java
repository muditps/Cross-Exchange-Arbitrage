package com.arbitrage.dashboard.analytics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Objects;

/**
 * Executes native analytics SQL against {@code arbitrage_opportunities} and
 * {@code simulation_results} (both TimescaleDB hypertables).
 *
 * <p><strong>Why JdbcTemplate, not JPA?</strong> These are aggregate time-series queries
 * ({@code date_trunc}, {@code FILTER}, {@code INTERVAL}) — they don't map to JPA entities.
 * JdbcTemplate gives direct access to the full PostgreSQL/TimescaleDB SQL dialect without
 * forcing an entity model onto analytics-only views.
 *
 * <p><strong>Why blocking?</strong> JdbcTemplate is blocking JDBC. Callers (the controller)
 * must wrap invocations in {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())}
 * to avoid blocking the Netty I/O thread pool in this WebFlux application. Analytics endpoints
 * are called occasionally (frontend polls every 30s) — {@code boundedElastic} is sized for
 * exactly this kind of occasional, bounded blocking work.
 *
 * <p><strong>NULL safety:</strong> {@code COALESCE} guards every aggregate that could return
 * NULL on an empty table ({@code AVG}, {@code MAX}). {@code RowMapper} uses
 * {@code getDouble}/{@code getLong} which return 0.0/0L on NULL, matching the DTO defaults.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class AnalyticsRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String SUMMARY_SQL = """
            SELECT
              COUNT(*)                                                    AS total_opportunities,
              COUNT(*) FILTER (WHERE status = 'CLOSED')                  AS closed_opportunities,
              COUNT(*) FILTER (WHERE status = 'EXPIRED')                 AS expired_opportunities,
              COUNT(*) FILTER (WHERE status IN ('DETECTED', 'OPEN'))     AS active_opportunities,
              COALESCE(ROUND(AVG(net_spread_bps)::NUMERIC, 2), 0)        AS avg_net_spread_bps,
              COALESCE(ROUND(MAX(net_spread_bps)::NUMERIC, 2), 0)        AS max_net_spread_bps
            FROM arbitrage_opportunities
            WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            """;

    private static final String SIMULATION_SQL = """
            SELECT
              COUNT(*)                                          AS total_simulations,
              COUNT(*) FILTER (WHERE was_profitable = true)    AS profitable_simulations
            FROM simulation_results
            WHERE simulation_timestamp >= NOW() - (? || ' hours')::INTERVAL
            """;

    private static final String HOURLY_SQL = """
            SELECT
              time_bucket(INTERVAL '1 hour', detection_timestamp)  AS hour,
              COUNT(*)                                              AS count
            FROM arbitrage_opportunities
            WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            GROUP BY 1
            ORDER BY 1 ASC
            """;

    private static final String EXCHANGE_PAIR_SQL = """
            SELECT
              buy_exchange,
              sell_exchange,
              COUNT(*)                                          AS count,
              COALESCE(ROUND(AVG(net_spread_bps)::NUMERIC, 2), 0) AS avg_net_spread_bps
            FROM arbitrage_opportunities
            WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            GROUP BY buy_exchange, sell_exchange
            ORDER BY count DESC
            """;

    /**
     * Returns aggregate summary stats for the given lookback window.
     * Queries both {@code arbitrage_opportunities} and {@code simulation_results}.
     *
     * @param windowHours number of hours to look back from now
     * @return populated summary DTO; all counts are 0 when no data exists
     */
    public AnalyticsSummaryDto getSummary(final int windowHours) {
        log.debug("analytics.summary.query windowHours={}", windowHours);
        final String hours = String.valueOf(windowHours);

        final AnalyticsSummaryDto oppStats = jdbcTemplate.queryForObject(SUMMARY_SQL, (rs, rowNum) ->
                new AnalyticsSummaryDto(
                        rs.getLong("total_opportunities"),
                        rs.getLong("closed_opportunities"),
                        rs.getLong("expired_opportunities"),
                        rs.getLong("active_opportunities"),
                        rs.getDouble("avg_net_spread_bps"),
                        rs.getDouble("max_net_spread_bps"),
                        0L, 0L, 0.0, windowHours
                ), hours);

        final long[] simStats = new long[]{0L, 0L};
        jdbcTemplate.query(SIMULATION_SQL, rs -> {
            simStats[0] = rs.getLong("total_simulations");
            simStats[1] = rs.getLong("profitable_simulations");
        }, hours);

        final long totalSims = simStats[0];
        final long profitableSims = simStats[1];
        final double winRate = totalSims > 0 ? (double) profitableSims / totalSims * 100.0 : 0.0;

        Objects.requireNonNull(oppStats, "SUMMARY_SQL returned null row — windowHours=" + windowHours);
        return new AnalyticsSummaryDto(
                oppStats.totalOpportunities(),
                oppStats.closedOpportunities(),
                oppStats.expiredOpportunities(),
                oppStats.activeOpportunities(),
                oppStats.avgNetSpreadBps(),
                oppStats.maxNetSpreadBps(),
                totalSims,
                profitableSims,
                Math.round(winRate * 10.0) / 10.0,
                windowHours
        );
    }

    /**
     * Returns per-hour opportunity counts for the given lookback window.
     * Hours with zero opportunities are omitted (sparse results are fine for the bar chart).
     *
     * @param windowHours number of hours to look back from now
     * @return list of hourly counts, sorted ascending by hour; empty when no data
     */
    public List<HourlyCountDto> getHourlyCounts(final int windowHours) {
        log.debug("analytics.hourly.query windowHours={}", windowHours);
        return jdbcTemplate.query(HOURLY_SQL, (rs, rowNum) ->
                new HourlyCountDto(rs.getTimestamp("hour").toInstant().toString(), rs.getLong("count")),
                String.valueOf(windowHours));
    }

    /**
     * Returns opportunity counts grouped by buy/sell exchange pair for the given window.
     *
     * @param windowHours number of hours to look back from now
     * @return list sorted by count descending; empty when no data
     */
    public List<ExchangePairStatsDto> getExchangePairStats(final int windowHours) {
        log.debug("analytics.exchange-pair.query windowHours={}", windowHours);
        return jdbcTemplate.query(EXCHANGE_PAIR_SQL, (rs, rowNum) ->
                new ExchangePairStatsDto(
                        rs.getString("buy_exchange"),
                        rs.getString("sell_exchange"),
                        rs.getLong("count"),
                        rs.getDouble("avg_net_spread_bps")
                ), String.valueOf(windowHours));
    }

    private static final String SPREAD_DIST_SQL = """
            SELECT bucket_label, COUNT(*) AS count, bucket_order
            FROM (
              SELECT
                CASE
                  WHEN net_spread_bps < 0   THEN 'Negative'
                  WHEN net_spread_bps < 5   THEN '0–5 bps'
                  WHEN net_spread_bps < 10  THEN '5–10 bps'
                  WHEN net_spread_bps < 20  THEN '10–20 bps'
                  WHEN net_spread_bps < 50  THEN '20–50 bps'
                  ELSE                           '50+ bps'
                END AS bucket_label,
                CASE
                  WHEN net_spread_bps < 0   THEN 1
                  WHEN net_spread_bps < 5   THEN 2
                  WHEN net_spread_bps < 10  THEN 3
                  WHEN net_spread_bps < 20  THEN 4
                  WHEN net_spread_bps < 50  THEN 5
                  ELSE                           6
                END AS bucket_order
              FROM arbitrage_opportunities
              WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            ) b
            GROUP BY bucket_label, bucket_order
            ORDER BY bucket_order
            """;

    private static final String DURATION_DIST_SQL = """
            SELECT bucket_label, COUNT(*) AS count, bucket_order
            FROM (
              SELECT
                CASE
                  WHEN total_duration_ms < 50    THEN '0–50ms'
                  WHEN total_duration_ms < 100   THEN '50–100ms'
                  WHEN total_duration_ms < 200   THEN '100–200ms'
                  WHEN total_duration_ms < 500   THEN '200–500ms'
                  WHEN total_duration_ms < 1000  THEN '500ms–1s'
                  ELSE                                '1s+'
                END AS bucket_label,
                CASE
                  WHEN total_duration_ms < 50    THEN 1
                  WHEN total_duration_ms < 100   THEN 2
                  WHEN total_duration_ms < 200   THEN 3
                  WHEN total_duration_ms < 500   THEN 4
                  WHEN total_duration_ms < 1000  THEN 5
                  ELSE                                6
                END AS bucket_order
              FROM arbitrage_opportunities
              WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
                AND status IN ('CLOSED', 'EXPIRED')
            ) b
            GROUP BY bucket_label, bucket_order
            ORDER BY bucket_order
            """;

    /**
     * Builds the cumulative P&L query with time-bucket downsampling.
     *
     * <p>Downsampling is necessary because 24h of data can contain 30,000+ CLOSED records —
     * a LIMIT 500 on raw records only shows the oldest 1.5% of the window. Time-bucketing
     * aggregates profits per bucket first, then computes the running total over the bucketed
     * series. This gives a smooth chart showing the full window arc (e.g., 288 points for
     * 24h with 5-minute buckets).
     *
     * <p>Bucket sizes are chosen to keep output ≤ 400 points for any window:
     * <ul>
     *   <li>≤ 6h: 2-minute buckets (max 180 points)</li>
     *   <li>≤ 24h: 5-minute buckets (max 288 points)</li>
     *   <li>≤ 72h: 15-minute buckets (max 288 points)</li>
     *   <li>> 72h: 30-minute buckets (max 336 points for 168h)</li>
     * </ul>
     *
     * <p>The {@code detection_timestamp} column is {@code timestamp with time zone}, so
     * {@code time_bucket} returns {@code timestamptz}. JDBC's {@code getTimestamp().toInstant()}
     * correctly normalises it to UTC without timezone ambiguity.
     */
    private static String buildCumulativePnlSql(final int bucketMinutes) {
        return """
            WITH latest_sim AS (
              SELECT opportunity_id, net_profit,
                     ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY simulation_timestamp DESC) AS rn
              FROM simulation_results
            ),
            pnl_per_opp AS (
              SELECT
                ao.detection_timestamp,
                ao.estimated_profit::NUMERIC                AS gross,
                COALESCE(ls.net_profit::NUMERIC, 0)         AS sim
              FROM arbitrage_opportunities ao
              LEFT JOIN latest_sim ls ON ls.opportunity_id = ao.id AND ls.rn = 1
              WHERE ao.status = 'CLOSED'
                AND ao.detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            ),
            bucketed AS (
              SELECT
                time_bucket(('%d minutes')::INTERVAL, detection_timestamp) AS bucket,
                SUM(gross) AS bucket_gross,
                SUM(sim)   AS bucket_sim
              FROM pnl_per_opp
              GROUP BY 1
            )
            SELECT
              bucket                                                      AS ts,
              SUM(bucket_gross) OVER (ORDER BY bucket)                   AS cumulative_gross,
              SUM(bucket_sim)   OVER (ORDER BY bucket)                   AS cumulative_sim
            FROM bucketed
            ORDER BY bucket
            """.formatted(bucketMinutes);
    }

    /** Returns bucket size in minutes that keeps chart output ≤ 400 points. */
    private static int bucketMinutes(final int windowHours) {
        if (windowHours <= 6)  return 2;
        if (windowHours <= 24) return 5;
        if (windowHours <= 72) return 15;
        return 30;
    }

    // ORDER BY RANDOM() gives a representative cross-section across all latency values.
    // ORDER BY simulated_latency_ms ASC would always return only the lowest-latency rows
    // (all 48ms BINANCE↔BYBIT routes) before the LIMIT 1000 is reached, hiding the
    // 57ms KuCoin routes entirely. Random sampling prevents this clustering artefact.
    private static final String LATENCY_PROFITABILITY_SQL = """
            SELECT simulated_latency_ms, net_profit
            FROM simulation_results
            WHERE simulation_timestamp >= NOW() - (? || ' hours')::INTERVAL
            ORDER BY RANDOM()
            LIMIT 1000
            """;

    private static final String HEATMAP_SQL = """
            SELECT
              EXTRACT(DOW  FROM detection_timestamp AT TIME ZONE 'UTC')::INT  AS day_of_week,
              EXTRACT(HOUR FROM detection_timestamp AT TIME ZONE 'UTC')::INT  AS hour_of_day,
              ROUND(AVG(net_spread_bps)::NUMERIC, 2)                          AS avg_net_spread_bps,
              COUNT(*)                                                         AS count
            FROM arbitrage_opportunities
            WHERE detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
            GROUP BY 1, 2
            ORDER BY 1, 2
            """;

    private static final String FALSE_POSITIVE_RATE_SQL = """
            WITH latest_sim AS (
              SELECT opportunity_id, was_profitable,
                     ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY simulation_timestamp DESC) AS rn
              FROM simulation_results
            )
            SELECT
              time_bucket(INTERVAL '1 hour', ao.detection_timestamp)          AS hour,
              COUNT(*)                                                          AS total_count,
              COUNT(*) FILTER (
                WHERE ao.status = 'EXPIRED'
                   OR (ls.opportunity_id IS NOT NULL AND ls.was_profitable = false)
              )                                                                 AS false_positive_count
            FROM arbitrage_opportunities ao
            LEFT JOIN latest_sim ls ON ls.opportunity_id = ao.id AND ls.rn = 1
            WHERE ao.detection_timestamp >= NOW() - (? || ' hours')::INTERVAL
              AND ao.status IN ('CLOSED', 'EXPIRED')
            GROUP BY 1
            ORDER BY 1
            """;

    /**
     * Returns opportunity counts bucketed by net spread bps for the histogram chart.
     * Six buckets: Negative, 0–5, 5–10, 10–20, 20–50, 50+.
     *
     * @param windowHours number of hours to look back from now
     * @return list of buckets sorted by bucket order; empty rows omitted
     */
    public List<SpreadBucketDto> getSpreadDistribution(final int windowHours) {
        log.debug("analytics.spread-dist.query windowHours={}", windowHours);
        return jdbcTemplate.query(SPREAD_DIST_SQL, (rs, rowNum) ->
                new SpreadBucketDto(
                        rs.getString("bucket_label"),
                        rs.getLong("count"),
                        rs.getInt("bucket_order")
                ), String.valueOf(windowHours));
    }

    /**
     * Returns CLOSED/EXPIRED opportunity counts bucketed by total duration in ms.
     * Six buckets: 0–50ms, 50–100ms, 100–200ms, 200–500ms, 500ms–1s, 1s+.
     *
     * @param windowHours number of hours to look back from now
     * @return list of buckets sorted by bucket order; empty rows omitted
     */
    public List<DurationBucketDto> getDurationDistribution(final int windowHours) {
        log.debug("analytics.duration-dist.query windowHours={}", windowHours);
        return jdbcTemplate.query(DURATION_DIST_SQL, (rs, rowNum) ->
                new DurationBucketDto(
                        rs.getString("bucket_label"),
                        rs.getLong("count"),
                        rs.getInt("bucket_order")
                ), String.valueOf(windowHours));
    }

    /**
     * Returns a time-ordered series of cumulative P&amp;L points for CLOSED opportunities.
     *
     * <p>Uses time-bucket downsampling ({@link #buildCumulativePnlSql}) to show the full
     * window arc instead of raw LIMIT-capped records. Each point represents the cumulative
     * P&amp;L up to the end of its bucket (e.g., every 5 minutes for a 24h window).
     * Opportunities with no simulation result contribute 0 to {@code cumulativeSimProfit}.
     *
     * <p>The {@code bucket} column is {@code timestamptz} — JDBC normalises it to UTC
     * correctly via {@code getTimestamp().toInstant()}, avoiding the timezone shift that
     * {@code AT TIME ZONE 'UTC'} caused (which converted to a bare timestamp, then JDBC
     * re-applied the JVM local timezone, shifting timestamps by −5:30 in IST).
     *
     * @param windowHours number of hours to look back from now
     * @return list of P&amp;L points ordered ascending by bucket; may be empty
     */
    public List<CumulativePnlPointDto> getCumulativePnl(final int windowHours) {
        log.debug("analytics.cumulative-pnl.query windowHours={} bucketMinutes={}", windowHours, bucketMinutes(windowHours));
        final String sql = buildCumulativePnlSql(bucketMinutes(windowHours));
        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new CumulativePnlPointDto(
                        rs.getTimestamp("ts").toInstant().toString(),
                        rs.getBigDecimal("cumulative_gross").toPlainString(),
                        rs.getBigDecimal("cumulative_sim").toPlainString()
                ), String.valueOf(windowHours));
    }

    /**
     * Returns (latencyMs, netProfit) scatter points from simulation results.
     * Each point is one closed simulation — plotted on the Latency vs Profitability chart.
     * Capped at 1000 points (sufficient for a scatter chart in any 24h window).
     *
     * @param windowHours number of hours to look back from now
     * @return scatter points ordered by latencyMs ascending; may be empty
     */
    public List<LatencyProfitabilityPointDto> getLatencyProfitability(final int windowHours) {
        log.debug("analytics.latency-profit.query windowHours={}", windowHours);
        return jdbcTemplate.query(LATENCY_PROFITABILITY_SQL, (rs, rowNum) ->
                new LatencyProfitabilityPointDto(
                        rs.getLong("simulated_latency_ms"),
                        rs.getBigDecimal("net_profit").toPlainString()
                ), String.valueOf(windowHours));
    }

    /**
     * Returns heatmap cells grouped by day-of-week × hour-of-day (UTC).
     * Only buckets with at least one opportunity are returned (sparse); the frontend
     * renders missing cells as grey. Uses a 168-hour (7-day) default window so that
     * all 7 days of the week can appear in the grid.
     *
     * @param windowHours number of hours to look back (default 168 = 7 days)
     * @return cells ordered by day_of_week, hour_of_day ascending; may be empty
     */
    public List<HeatmapCellDto> getHeatmapData(final int windowHours) {
        log.debug("analytics.heatmap.query windowHours={}", windowHours);
        return jdbcTemplate.query(HEATMAP_SQL, (rs, rowNum) ->
                new HeatmapCellDto(
                        rs.getInt("day_of_week"),
                        rs.getInt("hour_of_day"),
                        rs.getDouble("avg_net_spread_bps"),
                        rs.getLong("count")
                ), String.valueOf(windowHours));
    }

    /**
     * Returns the hourly false positive rate for resolved opportunities.
     *
     * <p>A false positive is an opportunity that was DETECTED but was not actually
     * executable at a profit: either it expired (status=EXPIRED) before execution could
     * complete, or the execution simulator determined it would have lost money
     * (was_profitable=false). Only CLOSED and EXPIRED opportunities are counted —
     * DETECTED/OPEN are unresolved and cannot yet be classified.
     *
     * <p>The rate is computed in the repository as {@code falsePositiveCount / totalCount × 100}
     * and rounded to one decimal place to avoid division-by-zero on the frontend.
     *
     * @param windowHours number of hours to look back from now
     * @return rate points ordered by hour ascending; may be empty
     */
    public List<FalsePositiveRatePointDto> getFalsePositiveRate(final int windowHours) {
        log.debug("analytics.false-positive.query windowHours={}", windowHours);
        return jdbcTemplate.query(FALSE_POSITIVE_RATE_SQL, (rs, rowNum) -> {
            final long total = rs.getLong("total_count");
            final long falsePositives = rs.getLong("false_positive_count");
            final double rate = total > 0 ? (double) falsePositives / total * 100.0 : 0.0;
            return new FalsePositiveRatePointDto(
                    rs.getTimestamp("hour").toInstant().toString(),
                    total,
                    falsePositives,
                    Math.round(rate * 10.0) / 10.0
            );
        }, String.valueOf(windowHours));
    }
}
