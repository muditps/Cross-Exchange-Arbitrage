package com.arbitrage.dashboard.websocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes arbitrage opportunity lifecycle events from Kafka and broadcasts them
 * to all connected WebSocket dashboard clients via {@link OpportunityWebSocketHandler}.
 *
 * <p>Mirrors the design of {@link TickBroadcaster} — see that class for the full
 * rationale on raw JSON forwarding, no custom consumer factory, and the dedicated
 * consumer group strategy. This class applies the same decisions to the
 * {@code arbitrage-opportunities} topic.
 *
 * <p><strong>All lifecycle events are forwarded</strong> (DETECTED, OPEN, CLOSED, EXPIRED).
 * The frontend {@code useOpportunities} hook deduplicates by {@code id} — it merges
 * multiple events for the same opportunity into a single row that updates in place.
 * Filtering on the server would require stateful tracking; filtering on the client is
 * simpler and equally efficient at the volumes this system produces.
 *
 * <p><strong>Consumer group:</strong> {@code dashboard-ws-opportunities-group} —
 * independent from {@code execution-simulator-group}, which also consumes this topic.
 * Each group receives every message independently.
 *
 * <p><strong>No manual ack:</strong> The dashboard is a read-only observer.
 * Missing an event on restart means the opportunity row briefly shows stale state
 * until the next update. Auto-commit is appropriate.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OpportunityBroadcaster {

    private final OpportunityWebSocketHandler opportunityWebSocketHandler;

    /**
     * Receives an arbitrage opportunity lifecycle event from Kafka and forwards it
     * to all connected WebSocket clients.
     *
     * <p>The record value is the raw JSON serialised by the detection engine's
     * {@link org.springframework.kafka.support.serializer.JsonSerializer}. Forwarding
     * it directly avoids a redundant deserialise-then-serialise cycle.
     *
     * @param record the Kafka consumer record containing the raw opportunity JSON
     */
    @KafkaListener(
            topics = "arbitrage-opportunities",
            groupId = "dashboard-ws-opportunities-group"
    )
    public void onOpportunity(final ConsumerRecord<String, String> record) {
        log.debug("opportunity.received key={} partition={} offset={}", record.key(), record.partition(), record.offset());
        opportunityWebSocketHandler.broadcast(record.value());
    }
}
