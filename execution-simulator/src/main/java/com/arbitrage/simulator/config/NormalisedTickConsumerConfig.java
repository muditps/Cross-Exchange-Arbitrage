package com.arbitrage.simulator.config;

import com.arbitrage.common.model.NormalisedTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for ingesting {@link NormalisedTick}s into the
 * {@link com.arbitrage.simulator.service.HistoricalPriceStore}.
 *
 * <p>This class is a pure bean factory — no instance state, no message processing.
 * The {@code @KafkaListener} method lives in
 * {@link com.arbitrage.simulator.listener.NormalisedTickListener} so that Spring
 * can construct the {@code @Configuration} CGLIB proxy without conflating bean
 * declaration with runtime message routing.
 *
 * <p><b>Why a second consumer in this module?</b> The execution simulator needs two
 * independent data streams:
 * <ol>
 *   <li>{@code arbitrage-opportunities} — closed opportunities to simulate (handled by
 *       {@link SimulationKafkaConsumerConfig})</li>
 *   <li>{@code normalised-ticks} — price history for replay at execution time (this config)</li>
 * </ol>
 *
 * <p><b>Consumer group:</b> {@code price-store-consumer-group} — independent of
 * {@code execution-simulator-group}. Each group receives its own copy of every message.
 *
 * <p><b>Why {@code auto-offset-reset=latest}?</b> The ring buffer is empty on startup.
 * Replaying historical ticks would fill the buffer, but the opportunity consumer also
 * starts from "latest" — the two streams would be out of sync until offsets converge,
 * producing meaningless simulation results. Starting both from "latest" keeps them in sync.
 *
 * <p><b>Why auto-commit?</b> Tick storage is purely in-memory — no external I/O,
 * no failure mode that requires reprocessing. Manual commit adds complexity with no benefit.
 *
 * <p><b>Why {@code max-poll-records=500}?</b> Tick processing is O(1) in-memory work
 * (deque add + optional head eviction) — microseconds per tick. Large poll batches
 * minimise per-record Kafka overhead and allow the buffer to fill quickly after restart.
 *
 * <p><b>Bean naming:</b> Prefixed with {@code priceStore} to avoid collision with
 * {@code simulation*} beans from {@link SimulationKafkaConsumerConfig} when both configs
 * are scanned by {@code DashboardApiApplication}.
 */
@Configuration
public class NormalisedTickConsumerConfig {

    /** Consumer group for the price store feed — independent of the opportunity consumer. */
    public static final String CONSUMER_GROUP = "price-store-consumer-group";

    /** Topic produced by the normalisation engine — one unified stream for all exchanges. */
    public static final String TOPIC_NORMALISED_TICKS = "normalised-ticks";

    /**
     * Creates a consumer factory for deserialising {@link NormalisedTick} messages.
     *
     * <p>Uses the same {@code setUseTypeHeaders(false)} / explicit target type pattern
     * as {@link SimulationKafkaConsumerConfig} to prevent {@code __TypeId__} Kafka header
     * conflicts when multiple Spring modules share the same Kafka cluster.
     *
     * @param kafkaProperties auto-bound Spring Boot Kafka properties (bootstrap-servers, etc.)
     * @param objectMapper    shared Jackson ObjectMapper with JavaTimeModule for Instant support
     * @return configured ConsumerFactory for NormalisedTick messages
     */
    @Bean
    public ConsumerFactory<String, NormalisedTick> priceStoreConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        // latest: ring buffer is always rebuilt from live ticks after restart
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Auto-commit: in-memory store updates have no failure mode requiring reprocessing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 500 records: tick processing is microseconds — large batches maximise throughput
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        JsonDeserializer<NormalisedTick> valueDeserializer =
                new JsonDeserializer<>(NormalisedTick.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    /**
     * Creates a {@link ConcurrentKafkaListenerContainerFactory} for the price store listener.
     *
     * <p>Concurrency of 3 matches the expected partition count of {@code normalised-ticks},
     * ensuring all partitions are consumed in parallel with no idle threads.
     *
     * @param priceStoreConsumerFactory the configured consumer factory
     * @return a container factory for price store listener registration
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalisedTick>
            priceStoreListenerContainerFactory(
                    ConsumerFactory<String, NormalisedTick> priceStoreConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, NormalisedTick> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(priceStoreConsumerFactory);
        factory.setConcurrency(3);
        return factory;
    }
}
