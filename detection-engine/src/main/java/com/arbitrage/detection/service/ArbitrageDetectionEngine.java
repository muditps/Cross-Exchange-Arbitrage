package com.arbitrage.detection.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.LatencyContext;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.common.util.KafkaHeaderUtils;
import com.arbitrage.detection.config.DetectionKafkaConsumerConfig;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.config.FeeConfiguration;
import com.arbitrage.detection.model.PriceState;
import com.arbitrage.detection.model.SpreadCalculationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Core arbitrage detection engine.
 *
 * <p>For every incoming normalised tick, this engine:
 * <ol>
 *   <li>Writes the tick's price state to Redis ({@link PriceStateService#storeTick}).</li>
 *   <li>Reads back the current best bid/ask for all exchanges for the same trading pair
 *       ({@link PriceStateService#getAllPricesForPair}) — a concurrent {@code HGETALL} per
 *       exchange, effectively one round-trip.</li>
 *   <li>Checks every ordered (sell, buy) exchange pair — both directions — using
 *       {@link SpreadCalculator}.</li>
 *   <li>Filters out: directions where either price is stale ({@link StalenessFilter}),
 *       directions with gross spread below the noise threshold
 *       ({@link DetectionProperties#getMinSpreadBps()}), and directions where net spread
 *       is zero or negative (fees exceed the gross spread).</li>
 *   <li>Builds an {@link ArbitrageOpportunity} for each genuinely profitable direction.</li>
 *   <li>Passes the profitable set to {@link OpportunityTracker#update} — new directions
 *       emit a DETECTED event; existing directions update peak/average silently; directions
 *       that vanished since the last tick emit a CLOSED event.</li>
 *   <li>Publishes DETECTED and CLOSED events to {@code arbitrage-opportunities} via
 *       {@link OpportunityKafkaPublisher} and records {@link DetectionMetrics}.</li>
 * </ol>
 *
 * <p><b>Pre-funded arbitrage model:</b> Balances exist on all exchanges simultaneously.
 * Both legs (buy on exchange B, sell on exchange A) are dispatched at the same instant.
 * No per-trade transfer fees — withdrawal fees only apply during periodic rebalancing.
 *
 * <p><b>Correctness invariants:</b>
 * <ul>
 *   <li>All spread and profit calculations use {@link BigDecimal} — never {@code double}.</li>
 *   <li>All comparisons use {@link BigDecimal#compareTo} — never {@code ==} or {@code >}.</li>
 *   <li>Both A→B and B→A directions are evaluated independently for every exchange pair.</li>
 *   <li>Staleness filtering is added in Session 3.4. Prices in Redis may be up to
 *       {@link DetectionProperties#getRedisPriceTtlMs()} old at this stage.</li>
 * </ul>
 *
 * <p><b>Threading:</b> The Kafka listener runs on Kafka's consumer thread pool (3 threads,
 * one per partition). Blocking on the reactive chain ({@code .block()}) is safe here —
 * we are not on a Reactor scheduler thread. The block duration is bounded by Redis RTT
 * (~1ms at local Redis) plus comparison logic (~µs).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ArbitrageDetectionEngine {

    private final PriceStateService priceStateService;
    private final SpreadCalculator spreadCalculator;
    private final DetectionProperties detectionProperties;
    private final StalenessFilter stalenessFilter;
    private final OpportunityTracker opportunityTracker;
    private final OpportunityKafkaPublisher opportunityPublisher;
    private final DetectionMetrics detectionMetrics;
    private final FeeConfiguration feeConfiguration;
    private final LatencyRecorder latencyRecorder;

    /**
     * Kafka listener entry point. Called for each normalised tick from all exchanges.
     *
     * <p>Reads T0–T5 latency headers forwarded by the normalisation engine, then stamps
     * T6 (consume time), T7 (after Redis update), and T8 (after comparison). Assembles a
     * {@link LatencyContext} and passes it to {@link #publishAndRecord} so T9 can be
     * stamped by the opportunity publisher.
     *
     * <p>The offset is acknowledged after the full pipeline completes (Redis write +
     * comparison). If an exception propagates out of this method, the offset is still
     * acknowledged to prevent infinite retry loops on malformed messages.
     *
     * @param record the Kafka consumer record including latency headers and tick value
     * @param ack    manual acknowledgment — committed after Redis write and comparison
     */
    @KafkaListener(
            topics = DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS,
            containerFactory = "detectionListenerContainerFactory"
    )
    public void onNormalisedTick(ConsumerRecord<String, NormalisedTick> record, Acknowledgment ack) {
        final long t6 = System.nanoTime(); // T6: detection consumer receives tick
        final NormalisedTick tick = record.value();

        final long t0 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T0);
        final long t1 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T1);
        final long t2 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T2);
        final long t3 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T3);
        final long t4 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T4);
        final long t5 = KafkaHeaderUtils.read(record.headers(), KafkaHeaderUtils.HDR_T5);

        // Single-element arrays let lambdas in the reactive chain capture and write these values.
        final long[] t7 = {0L};
        final long[] t8 = {0L};

        try {
            priceStateService.storeTick(tick)
                    .doOnNext(ignored -> t7[0] = System.nanoTime()) // T7: Redis state updated
                    .flatMap(stored -> priceStateService.getAllPricesForPair(tick.getTradingPair()))
                    .map(prices -> {
                        List<ArbitrageOpportunity> detections =
                                compareAllDirections(tick.getTradingPair(), prices);
                        t8[0] = System.nanoTime(); // T8: comparison complete
                        detectionMetrics.recordComparisonLatency(tick.getExchangeId(), t8[0] - t6);
                        return detections;
                    })
                    .map(detections -> opportunityTracker.update(tick.getTradingPair(), detections))
                    .doOnNext(events -> {
                        LatencyContext ctx = LatencyContext.builder()
                                .t0WebSocketReceived(t0)
                                .t1ParseStart(t1)
                                .t2RawTickPublished(t2)
                                .t3NormalisationReceived(t3)
                                .t4NormalisationComplete(t4)
                                .t5NormalisedTickPublished(t5)
                                .t6DetectionReceived(t6)
                                .t7RedisStateUpdated(t7[0])
                                .t8ComparisonComplete(t8[0])
                                .t9OpportunityPublished(0L) // stamped by OpportunityKafkaPublisher
                                .build();
                        publishAndRecord(events, tick, ctx);
                    })
                    .block();
        } catch (Exception ex) {
            log.error("Detection pipeline failed: pair={} exchange={} error={}",
                    tick.getTradingPair().canonicalSymbol(), tick.getExchangeId(), ex.getMessage(), ex);
        } finally {
            ack.acknowledge();
        }
    }

    /**
     * Publishes each lifecycle event to Kafka and records Micrometer metrics.
     *
     * <p>Called from the reactive chain in {@link #onNormalisedTick} with a full
     * {@link LatencyContext}. Also called from the expiry sweep with {@code null} context —
     * EXPIRED events have no pipeline origin and therefore no latency chain.
     *
     * @param events      lifecycle events produced by the opportunity tracker
     * @param tick        the tick that triggered this evaluation
     * @param latencyCtx  full T0–T8 context, or {@code null} for expiry-sweep events
     */
    private void publishAndRecord(List<ArbitrageOpportunity> events, NormalisedTick tick,
                                  LatencyContext latencyCtx) {
        if (events.isEmpty()) {
            log.debug("Detection complete: pair={} noEvents exchange={}",
                    tick.getTradingPair().canonicalSymbol(), tick.getExchangeId());
            return;
        }
        for (ArbitrageOpportunity event : events) {
            final long t9 = opportunityPublisher.publish(event, latencyCtx);
            detectionMetrics.recordEvent(event);
            if (latencyCtx != null) {
                latencyRecorder.record(latencyCtx.toBuilder().t9OpportunityPublished(t9).build());
            }
        }
        log.info("Lifecycle events: pair={} events={} exchange={}",
                tick.getTradingPair().canonicalSymbol(), events.size(), tick.getExchangeId());
    }

    /**
     * Runs the expiry sweep every 5 seconds, publishes EXPIRED events, and records metrics.
     *
     * <p>Moved here from {@link OpportunityTracker} so that publishing and metrics recording
     * are owned by the engine (the only class that knows about the publisher and metrics).
     * {@link OpportunityTracker} remains a pure state store.
     */
    @Scheduled(fixedDelay = 5000)
    void runExpiryAndPublish() {
        List<ArbitrageOpportunity> expired = opportunityTracker.expireStaleOpportunities();
        if (!expired.isEmpty()) {
            for (ArbitrageOpportunity event : expired) {
                opportunityPublisher.publish(event, null); // EXPIRED events have no pipeline latency context
                detectionMetrics.recordEvent(event);
            }
            log.warn("Expiry sweep: published {} EXPIRED opportunities", expired.size());
        }
    }

    /**
     * Evaluates all ordered (sell, buy) exchange pairs for arbitrage opportunities.
     *
     * <p>For 3 exchanges there are 6 ordered pairs (3 × 2), covering both directions
     * for each unordered pair. Each direction is evaluated independently — A→B and B→A
     * may both be profitable if the order books diverge in opposite directions
     * (extremely rare, but the system handles it correctly).
     *
     * <p>Three filters are applied before an opportunity is created:
     * <ol>
     *   <li><b>Staleness filter</b> ({@link StalenessFilter}): Rejects directions where
     *       either price is older than {@link DetectionProperties#getStalenessThresholdMs()}.
     *       Stale prices produce phantom signals — a spread that existed 200ms ago may have
     *       already closed. Applied first, before any BigDecimal arithmetic.</li>
     *   <li><b>Noise filter</b> ({@link DetectionProperties#getMinSpreadBps()}):
     *       Rejects directions where gross spread is below the configured minimum.
     *       Avoids computing fees for spreads that are obviously too small to be profitable.</li>
     *   <li><b>Profitability filter</b>: Rejects directions where net spread (after fees)
     *       is zero or negative. Most detected spreads on liquid pairs fail here.</li>
     * </ol>
     *
     * <p>Package-private to allow direct testing without going through the Kafka listener.
     *
     * @param pair   the trading pair being evaluated
     * @param prices map of live prices keyed by exchange — may contain fewer than all exchanges
     * @return list of profitable opportunities; empty if none detected
     */
    List<ArbitrageOpportunity> compareAllDirections(TradingPair pair, Map<ExchangeId, PriceState> prices) {
        if (prices.size() < 2) {
            log.debug("Skipping comparison: fewer than 2 live prices available pair={} liveExchanges={}",
                    pair.canonicalSymbol(), prices.keySet());
            return List.of();
        }

        List<ArbitrageOpportunity> opportunities = new ArrayList<>();
        BigDecimal minSpreadBpsBd = BigDecimal.valueOf(detectionProperties.getMinSpreadBps());

        for (ExchangeId sellExchangeId : ExchangeId.values()) {
            PriceState sellState = prices.get(sellExchangeId);
            if (sellState == null) continue;

            for (ExchangeId buyExchangeId : ExchangeId.values()) {
                if (buyExchangeId == sellExchangeId) continue;

                PriceState buyState = prices.get(buyExchangeId);
                if (buyState == null) continue;

                // Staleness gate — applied before any BigDecimal arithmetic
                if (stalenessFilter.isStale(sellState)) {
                    stalenessFilter.recordStaleSkip(sellExchangeId, pair, sellState);
                    continue;
                }
                if (stalenessFilter.isStale(buyState)) {
                    stalenessFilter.recordStaleSkip(buyExchangeId, pair, buyState);
                    continue;
                }

                SpreadCalculationResult result = spreadCalculator.calculate(
                        sellState, feeConfiguration.getTakerFeeRate(sellExchangeId),
                        buyState,  feeConfiguration.getTakerFeeRate(buyExchangeId));

                // Noise filter: skip tiny gross spreads that can never overcome fees
                if (result.getGrossSpreadBps().compareTo(minSpreadBpsBd) < 0) {
                    log.debug("Noise filter: pair={} sellExchange={} buyExchange={} grossSpreadBps={} threshold={}",
                            pair.canonicalSymbol(), sellExchangeId, buyExchangeId,
                            result.getGrossSpreadBps(), minSpreadBpsBd);
                    continue;
                }

                // Profitability filter: fees must not consume the entire spread
                if (result.getNetSpread().compareTo(BigDecimal.ZERO) <= 0) {
                    log.debug("Net-negative: pair={} sellExchange={} buyExchange={} grossSpread={} netSpread={}",
                            pair.canonicalSymbol(), sellExchangeId, buyExchangeId,
                            result.getGrossSpread(), result.getNetSpread());
                    continue;
                }

                ArbitrageOpportunity opportunity = buildOpportunity(
                        pair, sellExchangeId, sellState, buyExchangeId, buyState, result);
                opportunities.add(opportunity);

                log.info("Opportunity detected: buyExchange={} sellExchange={} pair={} " +
                                "grossSpreadBps={} netSpreadBps={} theoreticalProfit={}",
                        buyExchangeId, sellExchangeId, pair.canonicalSymbol(),
                        result.getGrossSpreadBps(), result.getNetSpreadBps(),
                        result.getTheoreticalProfit());
            }
        }

        return opportunities;
    }

    private ArbitrageOpportunity buildOpportunity(
            TradingPair pair,
            ExchangeId sellExchangeId, PriceState sellState,
            ExchangeId buyExchangeId, PriceState buyState,
            SpreadCalculationResult result) {

        Instant now = Instant.now();
        long nanoNow = System.nanoTime();

        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(pair)
                // Buy side: we buy on the exchange with the lower ask
                .buyExchange(buyExchangeId)
                .buyPrice(buyState.getBestAskPrice())
                .buyQuantity(buyState.getBestAskQuantity())
                .buyFeeRate(feeConfiguration.getTakerFeeRate(buyExchangeId))
                // Sell side: we sell on the exchange with the higher bid
                .sellExchange(sellExchangeId)
                .sellPrice(sellState.getBestBidPrice())
                .sellQuantity(sellState.getBestBidQuantity())
                .sellFeeRate(feeConfiguration.getTakerFeeRate(sellExchangeId))
                // Spread fields from calculator
                .grossSpread(result.getGrossSpread())
                .netSpread(result.getNetSpread())
                .grossSpreadBps(result.getGrossSpreadBps())
                .netSpreadBps(result.getNetSpreadBps())
                .arbitrageableQuantity(result.getArbitrageableQuantity())
                .theoreticalProfit(result.getTheoreticalProfit())
                // Lifecycle — DETECTED on first observation; state machine added in Session 3.5
                .status(OpportunityStatus.DETECTED)
                .detectionTimestamp(now)
                .lastUpdateTimestamp(now)
                .detectedNanoTime(nanoNow)
                .closedNanoTime(0L)
                // Tracking metrics — initialised to first observed values
                .peakNetSpread(result.getNetSpread())
                .averageNetSpread(result.getNetSpread())
                .totalDurationMs(0L)
                .build();
    }
}
