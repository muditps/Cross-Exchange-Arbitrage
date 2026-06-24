package com.arbitrage.connector;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.TradingPair;

/**
 * Strategy Pattern interface for exchange-specific Kafka tick publishers.
 *
 * <p>Each exchange connector has a corresponding {@code TickKafkaProducer} that
 * subscribes to the connector's {@code Flux<NormalisedTick>} and publishes each
 * tick to the exchange's dedicated Kafka topic (e.g., {@code raw-ticks-binance}).
 *
 * <p>The {@link com.arbitrage.connector.registry.ExchangeConnectorRegistry} injects
 * all implementations via Spring's list injection and calls {@link #start(TradingPair)}
 * for each configured trading pair on application startup. This enables:
 * <ul>
 *   <li>Adding a new exchange by implementing this interface — no registry changes needed</li>
 *   <li>Disabling exchanges via configuration without code changes</li>
 *   <li>Dynamic pair configuration from {@code arbitrage.trading.pairs} YAML</li>
 * </ul>
 *
 * <p><b>Threading model:</b> Implementations must be thread-safe. The registry may call
 * {@link #start(TradingPair)} from a different thread than {@link #stop()} — implementations
 * use {@code ConcurrentHashMap} for per-pair subscription tracking.
 *
 * @see com.arbitrage.connector.registry.ExchangeConnectorRegistry for lifecycle orchestration
 */
public interface TickKafkaProducer {

    /**
     * Starts publishing ticks for the given trading pair to Kafka.
     *
     * <p>Connects to the exchange for this pair (via the underlying
     * {@link ExchangeConnector}) and subscribes to the resulting
     * {@code Flux<NormalisedTick>}, forwarding each tick to Kafka.</p>
     *
     * <p>This method is idempotent per pair — calling it a second time for the
     * same pair has no effect if a subscription is already active for that pair.</p>
     *
     * <p><b>Single-pair connector limitation:</b> The underlying {@link ExchangeConnector}
     * implementations currently support one active pair at a time. Calling
     * {@code start(ETH-USDT)} while already started for {@code BTC-USDT} will log a
     * warning and skip the second pair. True multi-pair connector support is a Phase 3
     * enhancement requiring changes to the connector implementations.</p>
     *
     * @param tradingPair the pair to start publishing (e.g., BTC-USDT)
     */
    void start(TradingPair tradingPair);

    /**
     * Stops all active subscriptions and ceases publishing to Kafka.
     *
     * <p>Disposes all per-pair subscriptions. Does NOT disconnect the underlying
     * connector — other consumers may still be subscribed to the connector's Flux.</p>
     *
     * <p>This method is idempotent — calling it when already stopped has no effect.</p>
     */
    void stop();

    /**
     * Returns true if any subscription to the exchange's tick stream is currently active.
     *
     * @return true if at least one per-pair subscription is active and not disposed
     */
    boolean isRunning();

    /**
     * Returns the identifier of the exchange this producer publishes for.
     *
     * <p>Used by the registry for logging, status reporting, and targeted operations
     * (e.g., restarting a single exchange without affecting others).</p>
     *
     * @return the exchange identifier (e.g., {@link ExchangeId#BINANCE})
     */
    ExchangeId getExchangeId();
}
