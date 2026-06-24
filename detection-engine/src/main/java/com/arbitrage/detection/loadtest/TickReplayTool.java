package com.arbitrage.detection.loadtest;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.service.LatencyMetricsPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.arbitrage.detection.config.DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS;

/**
 * Load-testing tool that injects synthetic {@link NormalisedTick} messages directly into
 * the {@code normalised-ticks} Kafka topic at configurable rates.
 *
 * <p><b>Purpose:</b> Stress-test the detection pipeline in isolation to find the
 * "performance cliff" — the throughput at which end-to-end p99 exceeds 100ms.
 * The injection bypasses exchange connectors and the normalisation engine entirely,
 * so all observed latency comes from the detection engine alone.
 *
 * <p><b>Side effect on Redis:</b> Synthetic ticks overwrite live exchange prices in Redis
 * for the duration of the run. All price keys have a 10-second TTL, so the live system
 * recovers within 10 seconds after the run completes.
 *
 * <p><b>Concurrency:</b> Only one run may be active at a time. Concurrent calls to
 * {@link #runLoadTest} return immediately with {@code null} if a run is already in progress.
 */
@Component
@Slf4j
public class TickReplayTool {

    /** Burst mode: messages per burst cycle. */
    private static final int BURST_MESSAGES_PER_CYCLE = 1000;

    /** Burst mode: duration of each injection window in ms. */
    private static final long BURST_INJECTION_WINDOW_MS = 100;

    /** Burst mode: pause between bursts in ms. */
    private static final long BURST_PAUSE_MS = 5_000;

    /** p99 threshold in ms that defines the performance cliff. */
    private static final double PERFORMANCE_CLIFF_MS = 100.0;

    /** Default wait after injection before capturing final HdrHistogram snapshot (one publish cycle + buffer). */
    static final long DEFAULT_POST_RUN_SETTLE_MS = 12_000;

    private static final List<ExchangeId> EXCHANGES =
            List.of(ExchangeId.BINANCE, ExchangeId.BYBIT, ExchangeId.KUCOIN);

    private static final List<TradingPair> PAIRS = List.of(
            TradingPair.fromSymbol("BTC-USDT"),
            TradingPair.fromSymbol("ETH-USDT"),
            TradingPair.fromSymbol("BNB-USDT")
    );

    /** Realistic mid-prices per pair for synthetic tick generation. */
    private static final Map<String, BigDecimal> BASE_PRICES = Map.of(
            "BTC-USDT", new BigDecimal("67000.00000000"),
            "ETH-USDT", new BigDecimal("3800.00000000"),
            "BNB-USDT", new BigDecimal("580.00000000")
    );

    /** Half-spread as a fraction of mid-price (0.005% per side = 1bp round-trip). */
    private static final BigDecimal HALF_SPREAD_FRACTION = new BigDecimal("0.00005");

    private final KafkaTemplate<String, NormalisedTick> replayKafkaTemplate;
    private final LatencyMetricsPublisher latencyMetricsPublisher;
    private final long postRunSettleMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Random random = new Random();

    @Autowired
    public TickReplayTool(
            @Qualifier("replayNormalisedTickKafkaTemplate")
            KafkaTemplate<String, NormalisedTick> replayKafkaTemplate,
            LatencyMetricsPublisher latencyMetricsPublisher) {
        this(replayKafkaTemplate, latencyMetricsPublisher, DEFAULT_POST_RUN_SETTLE_MS);
    }

    /** Package-private constructor that overrides the post-run settle delay — for unit tests only. */
    TickReplayTool(
            KafkaTemplate<String, NormalisedTick> replayKafkaTemplate,
            LatencyMetricsPublisher latencyMetricsPublisher,
            long postRunSettleMs) {
        this.replayKafkaTemplate = replayKafkaTemplate;
        this.latencyMetricsPublisher = latencyMetricsPublisher;
        this.postRunSettleMs = postRunSettleMs;
    }

    /**
     * Runs a load test at the specified mode and duration.
     *
     * <p>Captures HdrHistogram percentile snapshots before and after injection,
     * then waits {@value #DEFAULT_POST_RUN_SETTLE_MS}ms for the detection pipeline to drain
     * and the next 10-second HdrHistogram snapshot to be published.
     *
     * @param mode             injection rate profile
     * @param durationSeconds  how long to inject ticks (not including settle time)
     * @return result with before/after latency percentiles, or {@code null} if already running
     */
    public ReplayResult runLoadTest(ReplayMode mode, int durationSeconds) {
        if (!running.compareAndSet(false, true)) {
            log.warn("Load test already in progress — ignoring concurrent request");
            return null;
        }
        try {
            return doRun(mode, durationSeconds);
        } finally {
            running.set(false);
        }
    }

    /** @return true if a load test is currently in progress */
    public boolean isRunning() {
        return running.get();
    }

    private ReplayResult doRun(ReplayMode mode, int durationSeconds) {
        log.info("Load test starting: mode={} duration={}s description={}",
                mode, durationSeconds, mode.getDescription());

        final Map<String, Map<String, Long>> before = latencyMetricsPublisher.getLatestPercentiles();

        final long startNs = System.nanoTime();
        final long endNs = startNs + (long) durationSeconds * 1_000_000_000L;
        long ticksProduced = 0;

        if (mode.isBurst()) {
            ticksProduced = runBurstMode(endNs);
        } else {
            ticksProduced = runSustainedMode(mode, endNs);
        }

        final long actualDurationMs = (System.nanoTime() - startNs) / 1_000_000;
        log.info("Load test injection complete: mode={} ticks={} duration={}ms throughput={}/sec — settling {}ms",
                mode, ticksProduced, actualDurationMs,
                String.format("%.0f", (double) ticksProduced / actualDurationMs * 1000),
                postRunSettleMs);

        sleepMs(postRunSettleMs);

        final Map<String, Map<String, Long>> after = latencyMetricsPublisher.getLatestPercentiles();
        final ReplayResult result = ReplayResult.of(
                mode, durationSeconds, actualDurationMs, ticksProduced, before, after);

        log.info("Load test complete: mode={} p99Before={}ms p99After={}ms cliffDetected={}",
                mode,
                String.format("%.1f", result.endToEndP99BeforeMs()),
                String.format("%.1f", result.endToEndP99AfterMs()),
                result.performanceCliffDetected());

        return result;
    }

    private long runSustainedMode(ReplayMode mode, long endNs) {
        final long intervalNs = 1_000_000_000L / mode.getTicksPerSecond();
        long ticksProduced = 0;
        int roundRobinIndex = 0;

        while (System.nanoTime() < endNs) {
            final long tickStart = System.nanoTime();
            final ExchangeId exchange = EXCHANGES.get(roundRobinIndex % EXCHANGES.size());
            final TradingPair pair = PAIRS.get(roundRobinIndex % PAIRS.size());
            sendTick(exchange, pair);
            ticksProduced++;
            roundRobinIndex++;

            final long elapsed = System.nanoTime() - tickStart;
            final long remaining = intervalNs - elapsed;
            if (remaining > 500_000) {
                parkNanos(remaining);
            }
        }
        return ticksProduced;
    }

    private long runBurstMode(long endNs) {
        long ticksProduced = 0;
        int roundRobinIndex = 0;

        while (System.nanoTime() < endNs) {
            final long burstStart = System.currentTimeMillis();
            for (int i = 0; i < BURST_MESSAGES_PER_CYCLE && System.nanoTime() < endNs; i++) {
                final ExchangeId exchange = EXCHANGES.get(roundRobinIndex % EXCHANGES.size());
                final TradingPair pair = PAIRS.get(roundRobinIndex % PAIRS.size());
                sendTick(exchange, pair);
                ticksProduced++;
                roundRobinIndex++;
            }
            final long burstMs = System.currentTimeMillis() - burstStart;
            log.debug("Burst cycle: {}ms for {} msgs — pausing {}ms",
                    burstMs, BURST_MESSAGES_PER_CYCLE, BURST_PAUSE_MS);
            sleepMs(BURST_PAUSE_MS);
        }
        return ticksProduced;
    }

    private void sendTick(ExchangeId exchange, TradingPair pair) {
        final NormalisedTick tick = generateSyntheticTick(exchange, pair);
        // Key mirrors NormalisationService: exchangeId + ":" + canonicalSymbol
        final String key = exchange.name() + ":" + pair.canonicalSymbol();
        replayKafkaTemplate.send(TOPIC_NORMALISED_TICKS, key, tick);
    }

    /**
     * Generates a synthetic tick with realistic prices and small Gaussian noise.
     *
     * <p>Noise magnitude is 0.1% of the base price — enough to occasionally create
     * cross-exchange spreads above the detection threshold, keeping the detection
     * pipeline active under load.
     */
    private NormalisedTick generateSyntheticTick(ExchangeId exchange, TradingPair pair) {
        final BigDecimal basePrice = BASE_PRICES.get(pair.canonicalSymbol());
        final BigDecimal noise = basePrice.multiply(
                BigDecimal.valueOf(random.nextGaussian() * 0.001)).setScale(8, RoundingMode.HALF_UP);
        final BigDecimal mid = basePrice.add(noise).max(BigDecimal.ONE).setScale(8, RoundingMode.HALF_UP);
        final BigDecimal halfSpread = mid.multiply(HALF_SPREAD_FRACTION).setScale(8, RoundingMode.HALF_UP);
        final long now = System.nanoTime();

        return NormalisedTick.builder()
                .exchangeId(exchange)
                .tradingPair(pair)
                .bestBidPrice(mid.subtract(halfSpread))
                .bestAskPrice(mid.add(halfSpread))
                .bestBidQuantity(BigDecimal.ONE)
                .bestAskQuantity(BigDecimal.ONE)
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(now)
                .processedTimestamp(now)
                .build();
    }

    private static void parkNanos(long nanos) {
        java.util.concurrent.locks.LockSupport.parkNanos(nanos);
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
