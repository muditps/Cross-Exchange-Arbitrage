package com.arbitrage.dashboard.opportunity;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

/**
 * REST endpoints for historical opportunity queries.
 *
 * <p>All endpoints are blocking JDBC calls wrapped in
 * {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())}
 * to keep the Netty I/O thread pool free.
 *
 * <p>The paginated list endpoint supports optional filters that map to URL params
 * retained by the frontend in the browser address bar — making filtered views
 * bookmarkable and shareable.
 */
@Tag(name = "Opportunities", description = "Historical closed/expired arbitrage opportunities and simulation results")
@RestController
@RequestMapping("/api/opportunities")
@Slf4j
@RequiredArgsConstructor
public class OpportunityController {

    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;

    private final OpportunityRepository opportunityRepository;

    /**
     * Returns a paginated list of CLOSED and EXPIRED opportunities.
     *
     * <p>Example: {@code GET /api/opportunities?pair=BTC-USDT&page=0&size=20}
     *
     * @param pair        optional trading pair filter (e.g. "BTC-USDT")
     * @param buyExchange optional buy-side exchange filter (e.g. "BINANCE")
     * @param sellExchange optional sell-side exchange filter
     * @param page        0-based page index (default 0)
     * @param size        page size, clamped to [1, 100] (default 20)
     */
    @Operation(summary = "List historical closed opportunities",
               description = "Paginated list of CLOSED/EXPIRED opportunities. All filter params are optional and are reflected in URL params for bookmarkable views.")
    @ApiResponse(responseCode = "200", description = "Paginated opportunity list with total count")
    @GetMapping
    public Mono<OpportunityPageDto> listClosed(
            @RequestParam(required = false) final String pair,
            @RequestParam(required = false) final String buyExchange,
            @RequestParam(required = false) final String sellExchange,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "" + DEFAULT_PAGE_SIZE) final int size) {

        final int clampedSize = Math.max(1, Math.min(size, MAX_PAGE_SIZE));
        final int clampedPage = Math.max(0, page);

        log.debug("opportunity.list pair={} buyExchange={} sellExchange={} page={} size={}",
                pair, buyExchange, sellExchange, clampedPage, clampedSize);

        return Mono.fromCallable(() -> {
            final long total = opportunityRepository.countClosed(pair, buyExchange, sellExchange);
            final var opportunities = opportunityRepository.findClosed(
                    pair, buyExchange, sellExchange, clampedPage, clampedSize);
            return new OpportunityPageDto(opportunities, total, clampedPage, clampedSize);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Returns a single opportunity by ID for the detail modal.
     *
     * <p>Example: {@code GET /api/opportunities/550e8400-e29b-41d4-a716-446655440000}
     *
     * @param id UUID string of the opportunity
     * @return the opportunity, or 404 if not found
     */
    @Operation(summary = "Get opportunity by ID",
               description = "Returns full opportunity details for the detail modal.")
    @ApiResponse(responseCode = "200", description = "Opportunity found")
    @ApiResponse(responseCode = "404", description = "Opportunity not found")
    @GetMapping("/{id}")
    public Mono<ClosedOpportunityDto> getById(@PathVariable final String id) {
        return Mono.fromCallable(() ->
                opportunityRepository.findById(id)
                        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Opportunity not found: " + id))
        ).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Returns the execution simulation result for an opportunity, if one exists.
     *
     * <p>Example: {@code GET /api/opportunities/550e8400-e29b-41d4-a716-446655440000/simulation}
     *
     * <p>Returns 204 No Content when no simulation exists (opportunity may not have been
     * processed by the execution simulator yet, or it expired without being simulated).
     *
     * @param id UUID string of the opportunity
     */
    @Operation(summary = "Get simulation result for an opportunity",
               description = "Returns the execution simulation outcome. 204 if no simulation exists for this opportunity.")
    @ApiResponse(responseCode = "200", description = "Simulation result found")
    @ApiResponse(responseCode = "204", description = "No simulation for this opportunity")
    @GetMapping("/{id}/simulation")
    public Mono<SimulationSummaryDto> getSimulation(@PathVariable final String id) {
        return Mono.fromCallable(() ->
                opportunityRepository.findSimulationByOpportunityId(id)
        ).subscribeOn(Schedulers.boundedElastic())
        .flatMap(opt -> opt.map(Mono::just)
                .orElse(Mono.empty()));
    }
}
