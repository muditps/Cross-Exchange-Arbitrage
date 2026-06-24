package com.arbitrage.simulator.config;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for the Execution Simulator.
 *
 * <p>Consumes closed opportunity events from the detection engine:
 * <ul>
 *   <li>{@code arbitrage-opportunities} — lifecycle events (DETECTED, CLOSED, EXPIRED)
 *       published by {@code OpportunityKafkaPublisher} in the detection engine</li>
 * </ul>
 *
 * <p><b>Consumer group:</b> {@code execution-simulator-group} — independent of the
 * detection engine group. Each group receives its own copy of every message, so the
 * simulator does not compete with any future consumer (e.g., a dashboard feed reader)
 * for opportunity events.
 *
 * <p><b>Why {@code auto-offset-reset=latest}?</b> On restart, replaying historical
 * opportunity events would trigger simulations against stale historical price data
 * (the {@code HistoricalPriceStore} ring buffer is empty on startup). Simulating
 * against an empty price store produces meaningless results. Starting from "now"
 * ensures every simulated opportunity has live replay data available.
 *
 * <p><b>Why {@code max-poll-records=100}?</b> Each simulation record involves a
 * TimescaleDB write (Session 4.5). At ~100ms per write, 100 records per poll keeps
 * the total poll processing time under 10 seconds — well within Kafka's default
 * {@code max.poll.interval.ms=300000}. Detection used 1000 because each record
 * cost only microseconds; simulation costs milliseconds.
 *
 * <p><b>Why concurrency=3?</b> {@code arbitrage-opportunities} has 3 partitions.
 * Three concurrent listener threads match partition count, preventing idle threads
 * and ensuring maximum throughput within the partition-ordering guarantee.
 *
 * <p><b>Bean naming:</b> Prefixed with {@code simulation} to avoid collision with
 * {@code detectionConsumerFactory} and {@code normalisationConsumerFactory} when all
 * modules are scanned by {@code DashboardApiApplication}.
 */
@Configuration
public class SimulationKafkaConsumerConfig {

    /** Consumer group identifier for all execution simulator instances. */
    public static final String CONSUMER_GROUP = "execution-simulator-group";

    /** Opportunity lifecycle topic produced by the detection engine. */
    public static final String TOPIC_ARBITRAGE_OPPORTUNITIES = "arbitrage-opportunities";

    /**
     * Creates a consumer factory for deserialising {@link ArbitrageOpportunity} messages.
     *
     * <p>Messages on {@code arbitrage-opportunities} are serialised by
     * {@link org.springframework.kafka.support.serializer.JsonSerializer} in the detection engine.
     * Symmetric {@link JsonDeserializer} is used here. {@code useHeadersIfPresent=false}
     * prevents the {@code __TypeId__} Kafka header from overriding our explicit target type —
     * matching the pattern proven in the detection pipeline integration test (Session 3.8).
     *
     * @param kafkaProperties Spring Boot auto-bound Kafka properties (bootstrap-servers, etc.)
     * @param objectMapper    shared Jackson ObjectMapper with JavaTimeModule for Instant support
     * @return configured ConsumerFactory for ArbitrageOpportunity messages
     */
    @Bean
    public ConsumerFactory<String, ArbitrageOpportunity> simulationConsumerFactory(
            KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        // latest: only simulate current opportunities — empty price store on restart makes
        // historical replay results meaningless
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Manual commit — offset committed only after TimescaleDB write succeeds
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 100 records per poll — each involves a DB write (~100ms), keeps processing
        // time well under max.poll.interval.ms
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        // --- Latency Tuning ---
        // Default fetch.max.wait.ms=500: broker holds the fetch response up to 500ms when
        // the topic is quiet between opportunity bursts. Reducing to 10ms matches the detection
        // consumer tuning (Session 6.4) and keeps the simulator's poll latency bounded.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 10);

        JsonDeserializer<ArbitrageOpportunity> valueDeserializer =
                new JsonDeserializer<>(ArbitrageOpportunity.class, objectMapper);
        valueDeserializer.addTrustedPackages("com.arbitrage.common.model");
        // Ignore __TypeId__ Kafka header — use explicit target type instead
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    /**
     * Creates a {@link ConcurrentKafkaListenerContainerFactory} for {@code @KafkaListener}
     * methods in the execution simulator.
     *
     * <p><b>AckMode.MANUAL_IMMEDIATE:</b> Offset committed only after the simulation
     * result has been persisted to TimescaleDB. If the DB write fails, the opportunity
     * event will be reprocessed on the next poll — preventing silent simulation gaps.
     *
     * @param simulationConsumerFactory the configured consumer factory
     * @return a container factory for simulation listener registration
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ArbitrageOpportunity>
            simulationListenerContainerFactory(
                    ConsumerFactory<String, ArbitrageOpportunity> simulationConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, ArbitrageOpportunity> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(simulationConsumerFactory);

        // 3 threads — one per arbitrage-opportunities partition
        factory.setConcurrency(3);

        // Manual offset commit — acknowledge only after TimescaleDB write
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
