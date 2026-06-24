package com.arbitrage.dashboard.health;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.normalisation.service.FeedHealthMonitor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

/**
 * REST endpoints for real-time exchange feed health and pipeline status.
 *
 * <p><strong>Why not Spring Actuator for exchange health?</strong> Spring Actuator's
 * {@code /actuator/health} endpoint is designed for infrastructure liveness (Kafka reachable?
 * Redis reachable?). It has no concept of per-exchange tick feed freshness. The
 * {@link FeedHealthMonitor} tracks per-exchange CONNECTED/STALE/DISCONNECTED transitions;
 * this controller exposes that state as a clean REST response the dashboard can consume
 * directly.
 *
 * <p><strong>Why is {@link FeedHealthMonitor} available here?</strong>
 * {@code dashboard-api} depends on {@code normalisation-engine} as a Gradle project dependency,
 * and {@code DashboardApiApplication} scans {@code com.arbitrage.*}. Spring therefore registers
 * {@link FeedHealthMonitor} as a bean and injects it here. This co-location of all modules in
 * one JVM is a deliberate choice for the development phase — see ADR-008.
 *
 * <p><strong>Blocking in WebFlux:</strong> {@link FeedHealthMonitor} reads from a
 * {@link java.util.concurrent.ConcurrentHashMap} — effectively instant. Wrapping in
 * {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())} is still correct
 * practice (the method signature could be changed later) and adds negligible overhead.
 */
@Tag(name = "Health", description = "Exchange feed health status and pipeline liveness. Infrastructure health (Kafka, Redis, TimescaleDB) is available via /actuator/health.")
@RestController
@RequestMapping("/api/health")
@Slf4j
@RequiredArgsConstructor
public class HealthController {

    private final FeedHealthMonitor feedHealthMonitor;

    @Operation(summary = "Get per-exchange feed health",
               description = "Returns CONNECTED/STALE/DISCONNECTED status and last-tick age for all exchanges. Polls FeedHealthMonitor — sub-millisecond read from ConcurrentHashMap.")
    @ApiResponse(responseCode = "200", description = "Health snapshot for all configured exchanges")
    /**
     * Returns the current feed health snapshot for all three exchanges.
     *
     * <p>Example: {@code GET /api/health/exchanges}
     *
     * <p>Response is an array of three {@link ExchangeHealthDto} objects — one per
     * {@link ExchangeId} value — regardless of whether a tick has ever been received.
     * Exchanges that have never produced a tick return {@code lastTickAgeMs: null}
     * and {@code status: "DISCONNECTED"}.
     *
     * <p>The frontend polls this every 5 seconds. Feed transitions (CONNECTED → STALE)
     * are visible in the UI within one poll interval.
     *
     * @return list of exchange health snapshots
     */
    @GetMapping("/exchanges")
    public Mono<List<ExchangeHealthDto>> getExchangeHealth() {
        return Mono.fromCallable(() ->
                Arrays.stream(ExchangeId.values())
                        .map(this::buildExchangeHealthDto)
                        .toList()
        ).subscribeOn(Schedulers.boundedElastic());
    }

    private ExchangeHealthDto buildExchangeHealthDto(final ExchangeId exchangeId) {
        final FeedStatus status = feedHealthMonitor.getStatus(exchangeId);
        final Long lastTickAgeMs = feedHealthMonitor.getLastSeenAgeMs(exchangeId)
                .isPresent()
                ? feedHealthMonitor.getLastSeenAgeMs(exchangeId).getAsLong()
                : null;

        log.debug("health.exchange exchangeId={} status={} lastTickAgeMs={}", exchangeId, status, lastTickAgeMs);

        return new ExchangeHealthDto(
                exchangeId.name(),
                status.name(),
                lastTickAgeMs,
                status == FeedStatus.CONNECTED
        );
    }
}
