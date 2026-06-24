package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.model.PriceKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * In-memory rolling buffer of recent {@link NormalisedTick}s, keyed by exchange and pair.
 *
 * <p><b>Purpose:</b> When the execution simulator determines that a trade filled at
 * T = detectionTime + executionLatencyMs, it needs to know the actual price at time T —
 * not the price at detection time. This store holds the last N seconds of ticks per
 * (exchange, pair) so {@link PriceAtExecutionLookup} can replay the market as it was
 * at any point within the window.
 *
 * <p><b>Data structure:</b> {@code ConcurrentLinkedDeque<NormalisedTick>} per {@link PriceKey}.
 * Ticks arrive roughly in time order (exchange timestamps increase monotonically within a feed),
 * so new ticks are appended to the tail and stale ticks are evicted from the head. Both
 * operations are O(1).
 *
 * <p><b>Eviction policy:</b> Triggered on every {@link #recordTick} call. Any tick at the
 * head of the deque whose {@code exchangeTimestamp} is older than
 * {@code historicalWindowSeconds} is removed. This piggybacks eviction onto the write path
 * with no separate scheduler — at a tick rate of 10–100/sec, the window stays bounded
 * without any additional thread.
 *
 * <p><b>Thread safety:</b> {@link ConcurrentHashMap} guarantees atomic {@code computeIfAbsent}
 * for bucket creation. {@link ConcurrentLinkedDeque} is a non-blocking concurrent queue —
 * reads and writes from multiple consumer threads are safe. The tail-add + head-evict pattern
 * is safe under concurrent access because the consumer threads never reorder ticks — each
 * thread appends its own tick and evicts its own stale entries from the head.
 *
 * <p><b>Window sizing:</b> Default 60 seconds (configured via
 * {@code arbitrage.simulation.historical-window-seconds}). At a tick rate of 100/sec per key
 * across 3 exchanges × 5 pairs = 15 keys, worst case memory is:
 * 15 × 6,000 ticks × ~250 bytes/tick ≈ 22 MB — acceptable heap overhead.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HistoricalPriceStore {

    private final SimulationProperties simulationProperties;

    private final ConcurrentHashMap<PriceKey, ConcurrentLinkedDeque<NormalisedTick>> priceHistory =
            new ConcurrentHashMap<>();

    /**
     * Records a tick in the rolling buffer for its (exchange, pair) key and evicts
     * any ticks that have fallen outside the configured historical window.
     *
     * <p>Eviction uses {@link NormalisedTick#getExchangeTimestamp()} as the age reference.
     * Exchange timestamps are wall-clock {@link Instant}s — comparable across JVM processes,
     * unlike {@code System.nanoTime()} which is process-local and non-transferable via Kafka.
     *
     * @param tick the incoming normalised tick to store
     */
    public void recordTick(NormalisedTick tick) {
        PriceKey key = new PriceKey(tick.getExchangeId(), tick.getTradingPair());
        ConcurrentLinkedDeque<NormalisedTick> deque =
                priceHistory.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
        deque.addLast(tick);
        evictStale(deque);
        log.debug("Tick recorded: exchange={} pair={} exchangeTimestamp={}",
                tick.getExchangeId(), tick.getTradingPair(), tick.getExchangeTimestamp());
    }

    /**
     * Returns an immutable snapshot of all ticks currently held for the given key,
     * ordered from oldest to newest.
     *
     * <p>Returns an empty list if no ticks have been recorded for this key.
     * The returned list is a snapshot — modifications to the store after this call
     * are not reflected in the returned list.
     *
     * @param exchangeId  the exchange to query
     * @param tradingPair the trading pair to query
     * @return immutable ordered snapshot, empty if no ticks are stored for this key
     */
    public List<NormalisedTick> getSnapshot(ExchangeId exchangeId, TradingPair tradingPair) {
        ConcurrentLinkedDeque<NormalisedTick> deque =
                priceHistory.get(new PriceKey(exchangeId, tradingPair));
        if (deque == null || deque.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(deque);
    }

    /**
     * Evicts stale ticks from the head of the deque.
     *
     * <p>A tick is stale when its {@code exchangeTimestamp} is older than
     * {@code now - historicalWindowSeconds}. Since ticks arrive in roughly
     * ascending timestamp order, all stale ticks are at the head.
     */
    private void evictStale(ConcurrentLinkedDeque<NormalisedTick> deque) {
        Instant evictionThreshold = Instant.now()
                .minusSeconds(simulationProperties.getHistoricalWindowSeconds());
        NormalisedTick head;
        while ((head = deque.peekFirst()) != null
                && head.getExchangeTimestamp().isBefore(evictionThreshold)) {
            deque.pollFirst();
        }
    }
}
