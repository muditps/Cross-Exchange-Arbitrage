package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Answers the question: "What was the price on exchange X for pair Y at time T?"
 *
 * <p><b>Context:</b> The detection engine records prices at detection time T₀.
 * The execution simulator determines that the simulated fill happened at
 * T₁ = T₀ + executionLatencyMs. The price at T₁ is what the strategy actually
 * paid/received — it differs from T₀ prices because markets move during the
 * execution window.
 *
 * <p><b>Lookup algorithm:</b> A floor search — find the most recent tick whose
 * {@code exchangeTimestamp} is ≤ {@code targetTime}. This models the "last known
 * price before fill" — the price that would have been quoted on the exchange order
 * book at the moment the order hit the matching engine.
 *
 * <p><b>Implementation:</b> Iterates the {@link HistoricalPriceStore} snapshot from
 * newest to oldest (tail to head of the deque). The target time is typically close
 * to the most recent ticks (execution window is 27–52ms, well within the 60s window),
 * so the loop terminates in 1–5 iterations in practice.
 *
 * <p><b>Why not binary search?</b> The window holds at most ~6,000 ticks per key
 * (100 ticks/sec × 60s). A linear scan from the tail terminates after O(k) iterations
 * where k is the number of ticks newer than the target — almost always single-digit.
 * Binary search adds complexity (requires random access, index arithmetic) for no
 * practical gain at this scale.
 *
 * <p><b>Empty result semantics:</b> Returns {@link Optional#empty()} when:
 * <ul>
 *   <li>No ticks exist for this (exchange, pair) — store is empty or has never seen this key</li>
 *   <li>All stored ticks are newer than {@code targetTime} — the target is before the
 *       first recorded tick (can happen on startup when the buffer is not yet warm)</li>
 * </ul>
 * The caller (Session 4.4 {@code SlippageEstimator}) treats an empty result as
 * "unable to simulate — use detection-time price as fallback."
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PriceAtExecutionLookup {

    private final HistoricalPriceStore historicalPriceStore;

    /**
     * Returns the most recent tick for the given (exchange, pair) whose
     * {@code exchangeTimestamp} is at or before {@code targetTime}.
     *
     * <p>This is a floor lookup: given a target instant, return the last price
     * that was known before or at that instant. The returned tick represents
     * the market state that an order arriving at {@code targetTime} would have
     * seen on the exchange order book.
     *
     * @param exchangeId  the exchange to look up prices on
     * @param tradingPair the trading pair to look up
     * @param targetTime  the simulated fill time (detectionTimestamp + executionLatencyMs)
     * @return the most recent tick at or before targetTime, or empty if the store
     *         holds no ticks for this key or all ticks are newer than targetTime
     */
    public Optional<NormalisedTick> findClosestBefore(ExchangeId exchangeId,
                                                      TradingPair tradingPair,
                                                      Instant targetTime) {
        List<NormalisedTick> snapshot = historicalPriceStore.getSnapshot(exchangeId, tradingPair);

        if (snapshot.isEmpty()) {
            log.debug("Price store empty for exchange={} pair={} — buffer not yet warm",
                    exchangeId, tradingPair);
            return Optional.empty();
        }

        // Iterate from newest to oldest — target is typically within the last few ticks
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            NormalisedTick tick = snapshot.get(i);
            if (!tick.getExchangeTimestamp().isAfter(targetTime)) {
                log.debug("Price found: exchange={} pair={} targetTime={} tickTime={} bidAsk={}/{}",
                        exchangeId, tradingPair, targetTime,
                        tick.getExchangeTimestamp(), tick.getBestBidPrice(), tick.getBestAskPrice());
                return Optional.of(tick);
            }
        }

        log.debug("No tick at or before targetTime={} for exchange={} pair={} — buffer not yet warm",
                targetTime, exchangeId, tradingPair);
        return Optional.empty();
    }
}
