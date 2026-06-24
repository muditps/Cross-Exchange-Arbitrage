package com.arbitrage.connector.bybit;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.common.util.KafkaHeaderUtils;
import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.TickKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Publishes raw Bybit ticks to the {@code raw-ticks-bybit} Kafka topic.
 *
 * <p><b>Pipeline position:</b>
 * <pre>
 * Bybit WS → BybitConnector → BybitTickKafkaProducer → Kafka → NormalisationEngine
 * </pre>
 *
 * <p><b>Message key:</b> The canonical trading pair (e.g., "BTC-USDT"). Kafka
 * partitions by key hash, so all ticks for the same pair land in the same partition.
 * This preserves chronological ordering per pair — the normalisation engine processes
 * one partition = one pair without cross-pair ordering complexity.</p>
 *
 * <p><b>Multi-pair tracking:</b> Subscriptions are tracked per trading pair in a
 * {@link ConcurrentHashMap} keyed by canonical symbol. See
 * {@link com.arbitrage.connector.binance.BinanceTickKafkaProducer} for the full
 * design rationale — all three producers follow the same pattern.</p>
 *
 * <p><b>Error handling:</b> Kafka send failures are logged with full context but do NOT
 * crash the subscription. One failed send must not stop the feed.</p>
 *
 * @see com.arbitrage.connector.config.KafkaProducerConfig for producer tuning (acks, batching, compression)
 * @see BybitConnector for the tick source
 * @see com.arbitrage.connector.TickKafkaProducer for the interface contract
 */
@Component
@Slf4j
public class BybitTickKafkaProducer implements TickKafkaProducer {

    /** Kafka topic for raw Bybit ticks. One topic per exchange. */
    static final String TOPIC = "raw-ticks-bybit";

    private final BybitConnector bybitConnector;
    private final KafkaTemplate<String, NormalisedTick> kafkaTemplate;
    private final boolean isEnabled;

    /**
     * Per-pair subscriptions. Key: canonical symbol (e.g., "BTC-USDT").
     * ConcurrentHashMap ensures thread-safe access when start/stop are called
     * concurrently from the registry and other callers.
     */
    private final ConcurrentMap<String, Disposable> subscriptionsByPair = new ConcurrentHashMap<>();

    /**
     * Creates a Kafka producer for Bybit ticks.
     *
     * @param bybitConnector the connector producing ticks
     * @param kafkaTemplate  the Kafka template for sending messages
     * @param properties     exchange connector properties (to check if Bybit is enabled)
     */
    @Autowired
    public BybitTickKafkaProducer(BybitConnector bybitConnector,
                                  @org.springframework.beans.factory.annotation.Qualifier("tickKafkaTemplate") KafkaTemplate<String, NormalisedTick> kafkaTemplate,
                                  ExchangeConnectorProperties properties) {
        this.bybitConnector = bybitConnector;
        this.kafkaTemplate = kafkaTemplate;

        ExchangeConnectorProperties.ExchangeProperties bybitConfig =
                properties.getConnectors().get("bybit");
        this.isEnabled = bybitConfig != null && bybitConfig.isEnabled();
    }

    /**
     * Package-private constructor for testing — allows injecting dependencies directly.
     *
     * @param bybitConnector the connector producing ticks
     * @param kafkaTemplate  the Kafka template for sending messages
     * @param isEnabled      whether the producer should be active
     */
    BybitTickKafkaProducer(BybitConnector bybitConnector,
                           KafkaTemplate<String, NormalisedTick> kafkaTemplate,
                           boolean isEnabled) {
        this.bybitConnector = bybitConnector;
        this.kafkaTemplate = kafkaTemplate;
        this.isEnabled = isEnabled;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Starts publishing ticks for the given pair. If the connector is already
     * connected to a different pair (single-pair limitation), logs a warning and
     * skips to prevent duplicate subscriptions on the same Flux.</p>
     */
    @Override
    public void start(TradingPair tradingPair) {
        if (!isEnabled) {
            log.info("Bybit Kafka producer is disabled. Not starting for pair={}", tradingPair.canonicalSymbol());
            return;
        }

        String pairKey = tradingPair.canonicalSymbol();

        Disposable existing = subscriptionsByPair.get(pairKey);
        if (existing != null && !existing.isDisposed()) {
            log.warn("Bybit Kafka producer already running for pair={}. Ignoring start request.", pairKey);
            return;
        }

        if (!subscriptionsByPair.isEmpty()) {
            log.debug("Bybit Kafka producer already active for pairs={}. Skipping pair={}.",
                    subscriptionsByPair.keySet(), pairKey);
            return;
        }

        log.info("Starting Bybit Kafka producer: topic={} pair={}", TOPIC, pairKey);

        Disposable subscription = bybitConnector.connect(tradingPair)
                .subscribe(
                        this::publishTick,
                        error -> log.error("Bybit tick stream error: pair={} error={}", pairKey, error.getMessage(), error),
                        () -> log.info("Bybit tick stream completed: pair={}", pairKey)
                );

        subscriptionsByPair.put(pairKey, subscription);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Disposes all active per-pair subscriptions atomically.</p>
     */
    @Override
    public void stop() {
        if (subscriptionsByPair.isEmpty()) {
            return;
        }

        subscriptionsByPair.forEach((pair, subscription) -> {
            if (!subscription.isDisposed()) {
                subscription.dispose();
                log.info("Bybit Kafka producer stopped: pair={}", pair);
            }
        });

        subscriptionsByPair.clear();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRunning() {
        return subscriptionsByPair.values().stream().anyMatch(d -> !d.isDisposed());
    }

    /** {@inheritDoc} */
    @Override
    public ExchangeId getExchangeId() {
        return ExchangeId.BYBIT;
    }

    /**
     * Publishes a single tick to Kafka asynchronously.
     *
     * <p>The message key is the canonical trading pair (e.g., "BTC-USDT").
     * All ticks for the same pair go to the same Kafka partition, preserving ordering.</p>
     *
     * @param tick the normalised tick to publish
     */
    private void publishTick(NormalisedTick tick) {
        String key = tick.getTradingPair().canonicalSymbol();

        long t2 = System.nanoTime();

        ProducerRecord<String, NormalisedTick> record = new ProducerRecord<>(TOPIC, key, tick);
        KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T0,
                tick.getExchangeTimestamp().toEpochMilli() * 1_000_000L);
        KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T1, tick.getReceivedTimestamp());
        KafkaHeaderUtils.write(record.headers(), KafkaHeaderUtils.HDR_T2, t2);

        CompletableFuture<SendResult<String, NormalisedTick>> future = kafkaTemplate.send(record);

        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Failed to publish tick to Kafka: topic={} key={} error={}",
                        TOPIC, key, exception.getMessage());
            } else if (log.isDebugEnabled()) {
                log.debug("Tick published to Kafka: topic={} key={} partition={} offset={}",
                        TOPIC, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
