package com.arbitrage.connector.registry;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnector;
import com.arbitrage.connector.TickKafkaProducer;
import com.arbitrage.connector.TradingPairsProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Central registry that manages the lifecycle of all exchange connectors and their
 * associated Kafka producers.
 *
 * <p><b>Responsibilities:</b>
 * <ul>
 *   <li>Start all enabled producers for all configured trading pairs on application startup</li>
 *   <li>Stop all producers cleanly on application shutdown</li>
 *   <li>Provide a status view of all exchange feeds ({@link #getStatuses()})</li>
 *   <li>Allow lookup of a specific connector by {@link ExchangeId} ({@link #getConnector(ExchangeId)})</li>
 * </ul>
 *
 * <p><b>Spring list injection:</b> This class receives all {@link TickKafkaProducer} and
 * {@link ExchangeConnector} beans as lists via Spring's list injection. Adding a fourth
 * exchange (e.g., Coinbase, NSE) requires only implementing these two interfaces —
 * the registry picks them up automatically with no code changes here. This is the
 * Open/Closed Principle in action.</p>
 *
 * <p><b>Lifecycle management ({@link SmartLifecycle}):</b> Spring calls {@link #start()}
 * automatically after all beans are initialised, and {@link #stop()} on context shutdown
 * (e.g., JVM shutdown hook, application context close). {@link #isAutoStartup()} returns
 * {@code true}, so no manual wiring is needed. The phase is default (0), meaning this
 * starts before most other lifecycle components and stops after them — appropriate for
 * a data source that other components depend on.</p>
 *
 * <p><b>Trading pairs:</b> The pairs to monitor are configured via
 * {@code arbitrage.trading.pairs} in {@code application.yml}. Starting with
 * {@code [BTC-USDT]}; expand by adding pairs to the YAML without code changes.
 * See {@link TradingPairsProperties} for configuration details and the single-pair
 * connector limitation in Phase 2.</p>
 *
 * @see TickKafkaProducer for the producer interface
 * @see ExchangeConnector for the connector interface
 * @see TradingPairsProperties for pair configuration
 */
@Component
@Slf4j
public class ExchangeConnectorRegistry implements SmartLifecycle {

    private final List<TickKafkaProducer> producers;
    private final Map<ExchangeId, ExchangeConnector> connectorsByExchange;
    private final TradingPairsProperties tradingPairsProperties;

    /** Whether this registry has been started. Guards against double-start. */
    private volatile boolean running = false;

    /**
     * Creates the registry with all exchange connectors and producers.
     *
     * <p>Spring injects all {@link TickKafkaProducer} implementations into {@code producers}
     * and all {@link ExchangeConnector} implementations into {@code connectors} via list
     * injection. The connectors list is indexed into an {@link EnumMap} for O(1) status
     * lookups by {@link ExchangeId}.</p>
     *
     * @param producers             all TickKafkaProducer implementations (one per exchange)
     * @param connectors            all ExchangeConnector implementations (one per exchange)
     * @param tradingPairsProperties configured trading pairs from application.yml
     */
    public ExchangeConnectorRegistry(List<TickKafkaProducer> producers,
                                     List<ExchangeConnector> connectors,
                                     TradingPairsProperties tradingPairsProperties) {
        this.producers = producers;
        this.connectorsByExchange = connectors.stream()
                .collect(Collectors.toMap(
                        ExchangeConnector::getExchangeId,
                        Function.identity(),
                        (existing, duplicate) -> {
                            log.warn("Duplicate connector for exchangeId={}, keeping first", existing.getExchangeId());
                            return existing;
                        },
                        () -> new EnumMap<>(ExchangeId.class)
                ));
        this.tradingPairsProperties = tradingPairsProperties;

        log.info("ExchangeConnectorRegistry initialised: producers={} connectors={} exchanges={}",
                producers.size(),
                connectors.size(),
                connectorsByExchange.keySet());
    }

    /**
     * Starts all Kafka producers for all configured trading pairs.
     *
     * <p>Called automatically by Spring on application startup ({@link SmartLifecycle}).
     * For each {@link TickKafkaProducer}, calls {@link TickKafkaProducer#start(TradingPair)}
     * for every pair in {@link TradingPairsProperties#asTradingPairs()}. Disabled producers
     * log and skip internally — the registry does not need to check enabled state.</p>
     *
     * <p><b>Single-pair limitation:</b> Each connector currently supports one active pair.
     * With multiple pairs configured, only the first pair per producer will effectively
     * connect — subsequent pairs are skipped with a WARN log. This is a known limitation
     * documented in {@link TickKafkaProducer#start(TradingPair)} and addressed in Phase 3.</p>
     */
    @Override
    public void start() {
        if (running) {
            log.warn("ExchangeConnectorRegistry already running. Ignoring start request.");
            return;
        }

        List<TradingPair> pairs = tradingPairsProperties.asTradingPairs();
        log.info("Starting ExchangeConnectorRegistry: producers={} pairs={}", producers.size(), pairs);

        for (TickKafkaProducer producer : producers) {
            for (TradingPair pair : pairs) {
                log.debug("Starting producer: exchange={} pair={}", producer.getExchangeId(), pair.canonicalSymbol());
                producer.start(pair);
            }
        }

        running = true;
        log.info("ExchangeConnectorRegistry started: allProducers={} pairs={}",
                producers.stream().map(p -> p.getExchangeId().name()).toList(),
                pairs.stream().map(TradingPair::canonicalSymbol).toList());
    }

    /**
     * Stops all Kafka producers and disconnects all connectors.
     *
     * <p>Called automatically by Spring on context shutdown. Stops producers first
     * (ceases Kafka publishing), then disconnects connectors (closes WebSocket connections).
     * This ordering prevents partially-published ticks from reaching Kafka during teardown.</p>
     *
     * <p>Both operations are idempotent — double-calling stop has no effect.</p>
     */
    @Override
    public void stop() {
        if (!running) {
            return;
        }

        log.info("Stopping ExchangeConnectorRegistry: producers={}", producers.size());

        producers.forEach(producer -> {
            log.debug("Stopping producer: exchange={}", producer.getExchangeId());
            producer.stop();
        });

        connectorsByExchange.forEach((exchangeId, connector) -> {
            log.debug("Disconnecting connector: exchange={}", exchangeId);
            connector.disconnect();
        });

        running = false;
        log.info("ExchangeConnectorRegistry stopped");
    }

    /**
     * Returns the current feed status for all known exchanges.
     *
     * <p>Queries each {@link ExchangeConnector} for its current {@link FeedStatus}.
     * Used by health endpoints and the dashboard to display per-exchange connectivity.</p>
     *
     * @return a map from each exchange to its current feed status
     */
    public Map<ExchangeId, FeedStatus> getStatuses() {
        Map<ExchangeId, FeedStatus> statuses = new EnumMap<>(ExchangeId.class);
        connectorsByExchange.forEach((exchangeId, connector) ->
                statuses.put(exchangeId, connector.getFeedStatus()));
        return statuses;
    }

    /**
     * Returns the connector for the specified exchange, if registered.
     *
     * @param exchangeId the exchange to look up
     * @return the connector, or empty if no connector is registered for this exchange
     */
    public Optional<ExchangeConnector> getConnector(ExchangeId exchangeId) {
        return Optional.ofNullable(connectorsByExchange.get(exchangeId));
    }

    /**
     * Returns the producer for the specified exchange, if registered.
     *
     * @param exchangeId the exchange to look up
     * @return the producer, or empty if no producer is registered for this exchange
     */
    public Optional<TickKafkaProducer> getProducer(ExchangeId exchangeId) {
        return producers.stream()
                .filter(p -> p.getExchangeId() == exchangeId)
                .findFirst();
    }

    /**
     * Returns the list of all registered producers.
     *
     * <p>Primarily for testing and observability — prefer {@link #getStatuses()} for
     * health reporting and {@link #getProducer(ExchangeId)} for targeted operations.</p>
     *
     * @return unmodifiable view of all registered producers
     */
    public List<TickKafkaProducer> getProducers() {
        return List.copyOf(producers);
    }

    /** {@inheritDoc} — Returns true after {@link #start()} is called and before {@link #stop()} completes. */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@code true} so Spring starts this registry automatically as part of
     * the application context lifecycle, without requiring an explicit
     * {@link org.springframework.context.ApplicationRunner} trigger.</p>
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
