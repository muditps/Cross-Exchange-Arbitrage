package com.arbitrage.dashboard.analytics;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

/**
 * REST endpoints for dashboard analytics — historical opportunity and simulation statistics.
 *
 * <p>All endpoints accept an optional {@code hours} query parameter (default 24) that sets
 * the lookback window. The frontend calls these on a 30-second poll via TanStack Query,
 * not on every render.
 *
 * <p><strong>Blocking JDBC in a WebFlux context — why {@code boundedElastic}?</strong>
 * {@link AnalyticsRepository} uses {@code JdbcTemplate}, which is blocking JDBC.
 * WebFlux runs on a small Netty thread pool (one thread per CPU core). If a blocking call
 * executes on a Netty thread, it stalls all other requests sharing that thread — at 8 cores,
 * one slow DB query could stall 1/8 of the server's capacity.
 * {@code Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())} offloads the call
 * to the {@code boundedElastic} pool, which is designed for blocking I/O (defaults to
 * 10×CPU threads, grows on demand up to a cap). Netty threads stay free for WebSocket
 * multiplexing and reactive pipelines.
 *
 * <p>This is the documented Spring WebFlux pattern for legacy blocking integrations.
 * A future improvement would be to replace {@code JdbcTemplate} with R2DBC
 * ({@code spring-boot-starter-data-r2dbc}) for fully non-blocking DB access.
 */
@Tag(name = "Analytics", description = "Historical opportunity counts, spread statistics, and simulation win rates")
@RestController
@RequestMapping("/api/analytics")
@Slf4j
@RequiredArgsConstructor
public class AnalyticsController {

    private static final int DEFAULT_WINDOW_HOURS = 24;
    private static final int MAX_WINDOW_HOURS = 168;

    private final AnalyticsRepository analyticsRepository;

    @Operation(summary = "Get aggregate analytics summary",
               description = "Returns total opportunities, spread statistics, and simulation win rate for the given lookback window.")
    @ApiResponse(responseCode = "200", description = "Aggregate summary for the requested window")
    @GetMapping("/summary")
    public Mono<AnalyticsSummaryDto> getSummary(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.summary.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getSummary(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get per-hour opportunity counts",
               description = "Returns opportunity counts bucketed by hour for the timeline bar chart. Hours with zero opportunities are omitted.")
    @ApiResponse(responseCode = "200", description = "Hourly counts sorted ascending by hour")
    @GetMapping("/hourly")
    public Mono<List<HourlyCountDto>> getHourly(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.hourly.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getHourlyCounts(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get opportunity counts by exchange route",
               description = "Returns counts and average spread grouped by buy/sell exchange pair for the horizontal bar chart.")
    @ApiResponse(responseCode = "200", description = "Route stats sorted by count descending")
    @GetMapping("/by-exchange-pair")
    public Mono<List<ExchangePairStatsDto>> getByExchangePair(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.exchange-pair.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getExchangePairStats(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get net-spread distribution",
               description = "Returns opportunity counts bucketed by net spread bps. Six buckets: Negative, 0-5, 5-10, 10-20, 20-50, 50+. Used for the spread histogram chart.")
    @ApiResponse(responseCode = "200", description = "Spread distribution buckets ordered by bucket range")
    @GetMapping("/spread-distribution")
    public Mono<List<SpreadBucketDto>> getSpreadDistribution(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.spread-dist.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getSpreadDistribution(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get opportunity duration distribution",
               description = "Returns CLOSED/EXPIRED opportunity counts bucketed by total duration ms. Six buckets: 0-50ms, 50-100ms, 100-200ms, 200-500ms, 500ms-1s, 1s+.")
    @ApiResponse(responseCode = "200", description = "Duration distribution buckets ordered by bucket range")
    @GetMapping("/duration-distribution")
    public Mono<List<DurationBucketDto>> getDurationDistribution(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.duration-dist.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getDurationDistribution(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get cumulative simulated P&L over time",
               description = "Returns a time-ordered series of cumulative theoretical and simulated net P&L for CLOSED opportunities. Capped at 500 points.")
    @ApiResponse(responseCode = "200", description = "Cumulative P&L points ordered ascending by timestamp")
    @GetMapping("/cumulative-pnl")
    public Mono<List<CumulativePnlPointDto>> getCumulativePnl(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.cumulative-pnl.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getCumulativePnl(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get latency vs profitability scatter data",
               description = "Returns up to 1000 simulation results as (latencyMs, netProfit) points for the scatter chart. Each point is one closed simulation result. The frontend computes a linear regression trendline and breakeven latency annotation.")
    @ApiResponse(responseCode = "200", description = "Scatter points ordered by latency ascending")
    @GetMapping("/latency-profitability")
    public Mono<List<LatencyProfitabilityPointDto>> getLatencyProfitability(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.latency-profit.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getLatencyProfitability(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get profitability heatmap (7-day × 24-hour)",
               description = "Returns heatmap cells grouped by UTC day-of-week and hour-of-day. Default window is 168 hours (7 days) so all weekdays can appear. Sparse — empty cells are absent from the response.")
    @ApiResponse(responseCode = "200", description = "Heatmap cells ordered by day_of_week, hour_of_day ascending")
    @GetMapping("/heatmap")
    public Mono<List<HeatmapCellDto>> getHeatmap(
            @RequestParam(defaultValue = "168") final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.heatmap.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getHeatmapData(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Operation(summary = "Get false positive rate over time",
               description = "Returns hourly false positive rates for resolved opportunities. A false positive is an EXPIRED opportunity or a simulation result with was_profitable=false. Only CLOSED and EXPIRED opportunities are counted.")
    @ApiResponse(responseCode = "200", description = "False positive rate points ordered by hour ascending")
    @GetMapping("/false-positive-rate")
    public Mono<List<FalsePositiveRatePointDto>> getFalsePositiveRate(
            @RequestParam(defaultValue = "" + DEFAULT_WINDOW_HOURS) final int hours) {
        final int clampedHours = clamp(hours);
        log.debug("analytics.false-positive.request hours={}", clampedHours);
        return Mono.fromCallable(() -> analyticsRepository.getFalsePositiveRate(clampedHours))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** Clamps the hours parameter to [1, MAX_WINDOW_HOURS] to prevent runaway queries. */
    private int clamp(final int hours) {
        return Math.max(1, Math.min(hours, MAX_WINDOW_HOURS));
    }
}
