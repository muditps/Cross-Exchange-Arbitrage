package com.arbitrage.simulator.listener;

import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.simulator.config.NormalisedTickConsumerConfig;
import com.arbitrage.simulator.service.HistoricalPriceStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listener that feeds incoming {@link NormalisedTick}s into the {@link HistoricalPriceStore}.
 *
 * <p>Separated from {@link NormalisedTickConsumerConfig} by design: the config class declares
 * Spring beans (consumer factory, container factory) while this component handles message routing.
 * Mixing {@code @Configuration} bean declarations with {@code @KafkaListener} method processing
 * in the same class causes Spring to conflict when creating the CGLIB proxy for the config class
 * and simultaneously registering the listener endpoint — the two lifecycle phases interfere.
 *
 * <p>This listener is the only write path into the {@link HistoricalPriceStore} during normal
 * operation. It runs on threads managed by {@code priceStoreListenerContainerFactory}, which
 * uses a concurrency of 3 to match the partition count of the {@code normalised-ticks} topic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NormalisedTickListener {

    private final HistoricalPriceStore historicalPriceStore;

    /**
     * Receives a normalised tick from the {@code normalised-ticks} Kafka topic and
     * stores it in the rolling price history buffer.
     *
     * @param tick the incoming normalised tick from any exchange
     */
    @KafkaListener(
            topics = NormalisedTickConsumerConfig.TOPIC_NORMALISED_TICKS,
            groupId = NormalisedTickConsumerConfig.CONSUMER_GROUP,
            containerFactory = "priceStoreListenerContainerFactory"
    )
    public void onNormalisedTick(NormalisedTick tick) {
        historicalPriceStore.recordTick(tick);
    }
}
