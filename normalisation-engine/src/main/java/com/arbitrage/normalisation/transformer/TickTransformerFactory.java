package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Routes inbound ticks to the correct {@link TickTransformer} by {@link ExchangeId}.
 *
 * <p>This is the entry point for all ticks entering the normalisation engine.
 * The {@link com.arbitrage.normalisation.service.NormalisationService} calls
 * {@link #transform} for every tick consumed from a raw-ticks Kafka topic.
 *
 * <p><b>Why a factory?</b> Centralises routing logic. Without this class, the
 * {@code NormalisationService} would need a conditional block:
 * {@code if (exchangeId == BINANCE) binanceTransformer.transform(...) else if ...}.
 * A factory encapsulates that decision and makes adding a new exchange as simple as
 * implementing {@link TickTransformer} and registering it as a Spring bean — the factory
 * discovers it automatically via constructor injection.
 *
 * <p><b>Auto-discovery:</b> The constructor accepts a {@code List<TickTransformer>} which
 * Spring populates with all beans implementing {@link TickTransformer}. This means adding
 * a new exchange transformer (e.g., for Phase 7's NSE) requires zero changes to this class
 * — Spring injects the new bean automatically.
 *
 * <p><b>Latency:</b> {@link EnumMap} is used internally for O(1) lookup by ordinal.
 * No iteration, no hash computation — faster than {@code HashMap} for enum keys.
 *
 * <p><b>Thread safety:</b> The internal map is built once at construction and never
 * mutated. All operations on the map are read-only. Thread-safe without synchronisation.
 */
@Component
@Slf4j
public class TickTransformerFactory {

    private final Map<ExchangeId, TickTransformer> transformersByExchange;

    /**
     * Creates the factory and builds the routing map from all registered transformers.
     *
     * <p>Spring injects all beans implementing {@link TickTransformer} as a list.
     * Each transformer declares which exchange it handles via {@link TickTransformer#supports()}.
     * If two transformers declare the same exchange, the last one registered wins — this
     * is logged as a warning because it indicates a configuration error.</p>
     *
     * @param transformers all Spring-registered {@link TickTransformer} implementations
     */
    public TickTransformerFactory(List<TickTransformer> transformers) {
        transformersByExchange = new EnumMap<>(ExchangeId.class);

        for (TickTransformer transformer : transformers) {
            ExchangeId exchangeId = transformer.supports();
            if (transformersByExchange.containsKey(exchangeId)) {
                log.warn("Duplicate TickTransformer registered for exchange={}: replacing {} with {}",
                        exchangeId,
                        transformersByExchange.get(exchangeId).getClass().getSimpleName(),
                        transformer.getClass().getSimpleName());
            }
            transformersByExchange.put(exchangeId, transformer);
            log.info("Registered TickTransformer: exchange={} transformer={}",
                    exchangeId, transformer.getClass().getSimpleName());
        }

        log.info("TickTransformerFactory initialised with {} transformers: {}",
                transformersByExchange.size(), transformersByExchange.keySet());
    }

    /**
     * Routes the inbound tick to the correct transformer and returns the result.
     *
     * <p>Returns {@link Optional#empty()} if:</p>
     * <ul>
     *   <li>The tick is null</li>
     *   <li>No transformer is registered for the tick's exchange (unknown exchange)</li>
     *   <li>The selected transformer returns empty (invalid tick)</li>
     * </ul>
     *
     * @param inboundTick           the tick to transform
     * @param normalisationStartNanos T3 — {@code System.nanoTime()} when the normalisation
     *                              engine began processing this tick (for latency measurement)
     * @return the transformed tick, or empty if transformation failed or is not supported
     */
    public Optional<NormalisedTick> transform(NormalisedTick inboundTick, long normalisationStartNanos) {
        if (inboundTick == null) {
            log.warn("Received null tick in TickTransformerFactory");
            return Optional.empty();
        }

        ExchangeId exchangeId = inboundTick.getExchangeId();
        TickTransformer transformer = transformersByExchange.get(exchangeId);

        if (transformer == null) {
            log.warn("No TickTransformer registered for exchange={} — tick dropped: pair={}",
                    exchangeId, inboundTick.getTradingPair());
            return Optional.empty();
        }

        return transformer.transform(inboundTick, normalisationStartNanos);
    }

    /**
     * Returns true if a transformer is registered for the given exchange.
     *
     * <p>Used for health checks and startup validation in
     * {@link com.arbitrage.normalisation.service.NormalisationService}.</p>
     *
     * @param exchangeId the exchange to check
     * @return true if a transformer is available for this exchange
     */
    public boolean supports(ExchangeId exchangeId) {
        return transformersByExchange.containsKey(exchangeId);
    }
}
