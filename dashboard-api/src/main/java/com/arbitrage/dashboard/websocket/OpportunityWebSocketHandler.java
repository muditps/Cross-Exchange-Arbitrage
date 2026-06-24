package com.arbitrage.dashboard.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Spring WebFlux WebSocket handler that broadcasts real-time arbitrage opportunity JSON
 * to all connected dashboard clients.
 *
 * <p>Follows the same broadcast architecture as {@link TickWebSocketHandler}:
 * a {@link Sinks.Many} multicast sink is the message bus. {@link OpportunityBroadcaster}
 * calls {@link #broadcast(String)} on every Kafka message from {@code arbitrage-opportunities};
 * every active WebSocket session receives every lifecycle event.
 *
 * <p><strong>Why every lifecycle event (DETECTED, OPEN, CLOSED, EXPIRED)?</strong>
 * The frontend {@code useOpportunities} hook deduplicates by {@code id} — when the same
 * opportunity transitions from DETECTED to OPEN to CLOSED, the hook merges these into
 * a single row that updates in place. Sending all events to the browser is simpler
 * and more correct than filtering on the server (which would require stateful tracking).
 * At typical arbitrage rates (a few per minute), the volume is trivial.
 *
 * <p>See {@link TickWebSocketHandler} for full explanation of the sink variant choices
 * and session lifecycle mechanics — the design is identical.
 */
@Component
@Slf4j
public class OpportunityWebSocketHandler implements WebSocketHandler {

    private final Sinks.Many<String> opportunitySink = Sinks.many().multicast().onBackpressureBuffer(256, false);

    /**
     * Publishes a raw opportunity JSON string to all connected dashboard clients.
     *
     * <p>Called by {@link OpportunityBroadcaster} on every message from the
     * {@code arbitrage-opportunities} Kafka topic (DETECTED, OPEN, CLOSED, EXPIRED events).
     *
     * @param opportunityJson the serialised {@code ArbitrageOpportunity} JSON from Kafka
     */
    public void broadcast(final String opportunityJson) {
        final Sinks.EmitResult result = opportunitySink.tryEmitNext(opportunityJson);
        if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            log.warn("opportunity.broadcast.failed result={}", result);
        }
    }

    /**
     * Handles a new WebSocket connection from the dashboard's opportunity feed.
     *
     * @param session the newly connected WebSocket session
     * @return a Mono that completes when the session is closed
     */
    @Override
    public Mono<Void> handle(final WebSocketSession session) {
        log.debug("ws.opportunity.session.opened sessionId={}", session.getId());

        final Mono<Void> send = session.send(opportunitySink.asFlux().map(session::textMessage));
        final Mono<Void> receive = session.receive().then();

        return Mono.firstWithSignal(send, receive)
                .doFinally(signal ->
                        log.debug("ws.opportunity.session.closed sessionId={} signal={}", session.getId(), signal)
                );
    }
}
