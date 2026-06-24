package com.arbitrage.dashboard.websocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes normalised tick messages from Kafka and broadcasts them to all connected
 * WebSocket dashboard clients via {@link TickWebSocketHandler}.
 *
 * <p><strong>Why a separate class?</strong> The {@link TickWebSocketHandler} is a
 * Spring WebFlux component and cannot use {@code @KafkaListener} directly — it would
 * need to be a Spring Kafka listener container managed by the Kafka infrastructure,
 * which conflicts with the WebFlux reactive bean lifecycle. Separating concerns keeps
 * each class responsible for exactly one thing: handler manages WebSocket sessions,
 * broadcaster manages Kafka consumption.
 *
 * <p><strong>Consumer group:</strong> {@code dashboard-ws-ticks-group} — independent
 * from {@code detection-engine-group} and {@code price-store-consumer-group}. Kafka
 * delivers each message to one consumer per group, so using a dedicated group ensures
 * the dashboard always receives every tick regardless of what the backend modules consume.
 *
 * <p><strong>Auto-offset-reset=latest:</strong> Inherited from the Spring Boot
 * auto-configured {@code kafkaListenerContainerFactory} (which reads from
 * {@code application.yml}). On startup, the dashboard only shows current prices —
 * not a replay of historical ticks that might be hours old.
 *
 * <p><strong>String value:</strong> The Kafka value is the raw JSON string produced
 * by the normalisation engine's {@code JsonSerializer}. We forward it directly to the
 * WebSocket clients without re-parsing into a Java object and re-serialising — this
 * eliminates one unnecessary serialisation round-trip on the hot path.
 *
 * <p><strong>No manual offset acknowledgment:</strong> The dashboard is a read-only
 * observer. If a tick is missed (e.g., on restart), the next tick will overwrite stale
 * state in the dashboard. There is no correctness requirement for exactly-once delivery
 * here — auto-commit is acceptable.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class TickBroadcaster {

    private final TickWebSocketHandler tickWebSocketHandler;

    /**
     * Receives a normalised tick record from Kafka and forwards it to all WebSocket sessions.
     *
     * <p>The record value is already a JSON string (serialised by the normalisation engine's
     * {@link org.springframework.kafka.support.serializer.JsonSerializer}). Forwarding it
     * directly avoids a redundant deserialise-then-serialise cycle on the hot path.
     *
     * <p>Timing: this method is called on the Kafka consumer thread. The
     * {@link TickWebSocketHandler#broadcast} call is non-blocking (sink emission),
     * so this method completes in microseconds and never holds up the Kafka poll loop.
     *
     * @param record the Kafka consumer record containing the raw tick JSON
     */
    @KafkaListener(
            topics = "normalised-ticks",
            groupId = "dashboard-ws-ticks-group"
    )
    public void onTick(final ConsumerRecord<String, String> record) {
        log.debug("tick.received key={} partition={} offset={}", record.key(), record.partition(), record.offset());
        tickWebSocketHandler.broadcast(record.value());
    }
}
