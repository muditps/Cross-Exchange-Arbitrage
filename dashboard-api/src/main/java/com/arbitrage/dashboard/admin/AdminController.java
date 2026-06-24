package com.arbitrage.dashboard.admin;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Admin endpoints for data management operations.
 *
 * <p>These endpoints are destructive and irreversible. Intended for development use:
 * resetting data between JVM tuning experiments, clearing phantom records, or starting
 * fresh after a load test. In production these would be protected by authentication.
 *
 * <p>All endpoints run JDBC deletes on {@code boundedElastic} to avoid blocking the
 * Netty I/O thread pool in this WebFlux application.
 */
@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Admin", description = "Data management and cleanup operations (destructive)")
public class AdminController {

    private final DataCleanupRepository dataCleanupRepository;

    /**
     * Deletes all arbitrage opportunities and simulation results for the given trading pair.
     *
     * <p>Use this to reset data for a specific pair before a load test or JVM experiment
     * so that the analytics charts reflect only the new session's data.
     *
     * @param pair canonical pair symbol, e.g., "BTC-USDT", "ETH-USDT", "BNB-USDT"
     * @return counts of deleted rows from each table
     */
    @DeleteMapping("/data/pair/{pair}")
    @Operation(
            summary = "Delete all data for a trading pair",
            description = "Deletes simulation_results and arbitrage_opportunities for the given pair. Irreversible."
    )
    @ApiResponse(responseCode = "200", description = "Data deleted successfully")
    public Mono<ResponseEntity<DeleteResult>> deleteByPair(
            @PathVariable @Parameter(description = "Canonical pair e.g. BTC-USDT") final String pair) {

        log.warn("Admin DELETE /api/admin/data/pair/{} triggered", pair);
        return Mono.fromCallable(() -> dataCleanupRepository.deleteByPair(pair))
                .subscribeOn(Schedulers.boundedElastic())
                .map(ResponseEntity::ok);
    }

    /**
     * Deletes ALL arbitrage opportunities and simulation results across all trading pairs.
     *
     * <p>Full reset. Use before a fresh load test run or when phantom data from a Bybit
     * startup reconnect has contaminated multiple pairs.
     *
     * @return counts of deleted rows from each table
     */
    @DeleteMapping("/data/all")
    @Operation(
            summary = "Delete all data across all pairs",
            description = "Truncates simulation_results and arbitrage_opportunities entirely. Irreversible."
    )
    @ApiResponse(responseCode = "200", description = "All data deleted successfully")
    public Mono<ResponseEntity<DeleteResult>> deleteAll() {
        log.warn("Admin DELETE /api/admin/data/all triggered");
        return Mono.fromCallable(dataCleanupRepository::deleteAll)
                .subscribeOn(Schedulers.boundedElastic())
                .map(ResponseEntity::ok);
    }
}
