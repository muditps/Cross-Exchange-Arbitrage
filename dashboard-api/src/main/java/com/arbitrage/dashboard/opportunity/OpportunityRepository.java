package com.arbitrage.dashboard.opportunity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Queries historical opportunities and simulation results from TimescaleDB.
 *
 * <p>Uses {@code JdbcTemplate} (blocking JDBC). All callers must wrap invocations in
 * {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())} to stay off
 * the Netty I/O thread pool. See {@link AnalyticsRepository} for the same pattern.
 *
 * <p>Optional filter parameters are applied via dynamic SQL construction — parameters
 * that are blank/null are simply omitted from the WHERE clause. This avoids the
 * {@code COALESCE(?, col) = col} trick, which prevents index use on {@code trading_pair}
 * and {@code buy_exchange}.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class OpportunityRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String SELECT_COLUMNS = """
            SELECT id::text, trading_pair, buy_exchange, sell_exchange,
                   buy_price, sell_price, raw_spread_bps, net_spread_bps,
                   buy_fee_rate, sell_fee_rate, quantity, estimated_profit,
                   status, detection_timestamp, closed_timestamp,
                   peak_net_spread_bps, average_net_spread_bps, total_duration_ms
            FROM arbitrage_opportunities
            """;

    private static final RowMapper<ClosedOpportunityDto> OPPORTUNITY_MAPPER = (rs, rowNum) -> {
        final Timestamp closedTs = rs.getTimestamp("closed_timestamp");
        return new ClosedOpportunityDto(
                rs.getString("id"),
                rs.getString("trading_pair"),
                rs.getString("buy_exchange"),
                rs.getString("sell_exchange"),
                rs.getBigDecimal("buy_price").toPlainString(),
                rs.getBigDecimal("sell_price").toPlainString(),
                rs.getBigDecimal("raw_spread_bps").toPlainString(),
                rs.getBigDecimal("net_spread_bps").toPlainString(),
                rs.getBigDecimal("buy_fee_rate").toPlainString(),
                rs.getBigDecimal("sell_fee_rate").toPlainString(),
                rs.getBigDecimal("quantity").toPlainString(),
                rs.getBigDecimal("estimated_profit").toPlainString(),
                rs.getString("status"),
                rs.getTimestamp("detection_timestamp").toInstant().atOffset(ZoneOffset.UTC).toString(),
                closedTs != null ? closedTs.toInstant().atOffset(ZoneOffset.UTC).toString() : null,
                nullableBigDecimal(rs, "peak_net_spread_bps"),
                nullableBigDecimal(rs, "average_net_spread_bps"),
                rs.getLong("total_duration_ms")
        );
    };

    private static final RowMapper<SimulationSummaryDto> SIMULATION_MAPPER = (rs, rowNum) ->
            new SimulationSummaryDto(
                    rs.getLong("simulated_latency_ms"),
                    rs.getBigDecimal("slippage_bps").toPlainString(),
                    rs.getBigDecimal("simulated_buy_price").toPlainString(),
                    rs.getBigDecimal("simulated_sell_price").toPlainString(),
                    rs.getBigDecimal("simulated_quantity").toPlainString(),
                    rs.getBoolean("was_profitable"),
                    rs.getBigDecimal("net_profit").toPlainString(),
                    rs.getBigDecimal("fill_probability").toPlainString()
            );

    /**
     * Returns a paginated list of CLOSED and EXPIRED opportunities, newest first.
     *
     * @param pair        trading pair filter (e.g. "BTC-USDT"); null/blank = all pairs
     * @param buyExchange buy-side exchange filter; null/blank = all exchanges
     * @param sellExchange sell-side exchange filter; null/blank = all exchanges
     * @param page        0-based page index
     * @param size        page size (clamped by caller to [1, 100])
     */
    public List<ClosedOpportunityDto> findClosed(
            final String pair, final String buyExchange, final String sellExchange,
            final int page, final int size) {
        final StringBuilder sql = new StringBuilder(SELECT_COLUMNS).append("WHERE status IN ('CLOSED', 'EXPIRED') ");
        final List<Object> params = buildFilterParams(sql, pair, buyExchange, sellExchange);
        sql.append("ORDER BY detection_timestamp DESC LIMIT ? OFFSET ?");
        params.add(size);
        params.add((long) page * size);

        log.debug("opportunity.find.closed pair={} buyEx={} sellEx={} page={} size={}", pair, buyExchange, sellExchange, page, size);
        return jdbcTemplate.query(sql.toString(), OPPORTUNITY_MAPPER, params.toArray());
    }

    /**
     * Returns the total count of CLOSED/EXPIRED opportunities matching the given filters.
     * Used to compute the total page count in the response envelope.
     */
    public long countClosed(final String pair, final String buyExchange, final String sellExchange) {
        final StringBuilder sql = new StringBuilder(
                "SELECT COUNT(*) FROM arbitrage_opportunities WHERE status IN ('CLOSED', 'EXPIRED') ");
        final List<Object> params = buildFilterParams(sql, pair, buyExchange, sellExchange);
        final Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }

    /**
     * Returns a single opportunity by its UUID string, regardless of status.
     * Used to populate the detail modal.
     */
    public Optional<ClosedOpportunityDto> findById(final String id) {
        final String sql = SELECT_COLUMNS + "WHERE id::text = ?";
        final List<ClosedOpportunityDto> results = jdbcTemplate.query(sql, OPPORTUNITY_MAPPER, id);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * Returns the most recent simulation result for the given opportunity, if one exists.
     */
    public Optional<SimulationSummaryDto> findSimulationByOpportunityId(final String opportunityId) {
        final String sql = """
                SELECT simulated_latency_ms, slippage_bps, simulated_buy_price, simulated_sell_price,
                       simulated_quantity, was_profitable, net_profit, fill_probability
                FROM simulation_results
                WHERE opportunity_id::text = ?
                ORDER BY simulation_timestamp DESC
                LIMIT 1
                """;
        final List<SimulationSummaryDto> results = jdbcTemplate.query(sql, SIMULATION_MAPPER, opportunityId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    private List<Object> buildFilterParams(
            final StringBuilder sql, final String pair,
            final String buyExchange, final String sellExchange) {
        final List<Object> params = new ArrayList<>();
        if (pair != null && !pair.isBlank()) {
            sql.append("AND trading_pair = ? ");
            params.add(pair);
        }
        if (buyExchange != null && !buyExchange.isBlank()) {
            sql.append("AND buy_exchange = ? ");
            params.add(buyExchange);
        }
        if (sellExchange != null && !sellExchange.isBlank()) {
            sql.append("AND sell_exchange = ? ");
            params.add(sellExchange);
        }
        return params;
    }

    private static String nullableBigDecimal(final java.sql.ResultSet rs, final String col) throws java.sql.SQLException {
        final java.math.BigDecimal val = rs.getBigDecimal(col);
        return val != null ? val.toPlainString() : null;
    }
}
