package com.arbitrage.dashboard.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Executes DELETE operations against {@code simulation_results} and
 * {@code arbitrage_opportunities} for the data-reset admin API.
 *
 * <p><b>Delete order matters:</b> {@code simulation_results.opportunity_id} references
 * {@code arbitrage_opportunities.id}. simulation_results must be deleted first to avoid
 * FK constraint violations.
 *
 * <p><b>Why a separate repository?</b> Analytics queries and destructive admin operations
 * belong in separate classes — mixing SELECTs and DELETEs in one class makes it easy to
 * accidentally call a delete from a read-only code path.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class DataCleanupRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Deletes all simulation results and opportunities for the given trading pair.
     * Pair format must match the {@code trading_pair} column (e.g., "BTC-USDT").
     *
     * @param tradingPair canonical trading pair symbol (e.g., "BTC-USDT")
     * @return {@link DeleteResult} with counts of deleted rows from each table
     */
    @Transactional
    public DeleteResult deleteByPair(final String tradingPair) {
        log.warn("DATA CLEANUP: deleting all records for pair={}", tradingPair);

        final int simDeleted = jdbcTemplate.update("""
                DELETE FROM simulation_results sr
                WHERE EXISTS (
                  SELECT 1 FROM arbitrage_opportunities ao
                  WHERE ao.id = sr.opportunity_id
                    AND ao.trading_pair = ?
                )
                """, tradingPair);

        final int oppDeleted = jdbcTemplate.update(
                "DELETE FROM arbitrage_opportunities WHERE trading_pair = ?",
                tradingPair);

        log.warn("DATA CLEANUP COMPLETE: pair={} simulationsDeleted={} opportunitiesDeleted={}",
                tradingPair, simDeleted, oppDeleted);

        return new DeleteResult(tradingPair, oppDeleted, simDeleted);
    }

    /**
     * Deletes ALL simulation results and opportunities across all pairs.
     * Intended for full test-run resets between JVM tuning experiments or load tests.
     *
     * @return {@link DeleteResult} with counts of deleted rows from each table
     */
    @Transactional
    public DeleteResult deleteAll() {
        log.warn("DATA CLEANUP: deleting ALL simulation_results and arbitrage_opportunities");

        final int simDeleted  = jdbcTemplate.update("DELETE FROM simulation_results");
        final int oppDeleted  = jdbcTemplate.update("DELETE FROM arbitrage_opportunities");

        log.warn("DATA CLEANUP COMPLETE: simulationsDeleted={} opportunitiesDeleted={}",
                simDeleted, oppDeleted);

        return new DeleteResult("ALL", oppDeleted, simDeleted);
    }
}
