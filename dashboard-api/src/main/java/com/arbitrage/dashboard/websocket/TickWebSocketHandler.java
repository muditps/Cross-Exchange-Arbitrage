package com.arbitrage.dashboard.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Spring WebFlux WebSocket handler that broadcasts real-time normalised tick JSON
 * to all connected dashboard clients.
 *
 * <p><strong>Broadcast architecture:</strong> Uses a Project Reactor {@link Sinks.Many}
 * multicast sink as the message bus. When {@link #broadcast(String)} is called (by the
 * Kafka consumer), the JSON string is emitted into the sink. Every active WebSocket
 * session subscribes to the same sink flux — they all receive every tick.
 *
 * <p><strong>Why {@code Sinks.many().multicast().onBackpressureBuffer()}?</strong>
 * <ul>
 *   <li>{@code multicast()} — all subscribers share the same upstream; each emission
 *       is delivered to every subscriber (fan-out). A {@code unicast()} sink only allows
 *       one subscriber.</li>
 *   <li>{@code onBackpressureBuffer()} — if a slow client can't consume as fast as ticks
 *       arrive, messages are buffered (256 elements by default) rather than the sink
 *       throwing an error. At 10–30 ticks/sec, the buffer is never at risk of filling.</li>
 *   <li>Not {@code replay()} — new subscribers only see ticks arriving after they connect.
 *       Replaying historical ticks would show stale prices in the dashboard.</li>
 * </ul>
 *
 * <p><strong>Session lifecycle:</strong> {@link #handle(WebSocketSession)} returns a
 * {@code Mono<Void>} from {@code session.send(flux)}. Spring WebFlux keeps the connection
 * open for as long as that Mono runs. When the browser disconnects, Reactor-Netty closes
 * the channel, which terminates the send flux subscription — cleanly ending the session
 * without any explicit session registry or cleanup logic.
 *
 * <p><strong>Thread safety:</strong> {@link Sinks.Many#tryEmitNext} is thread-safe.
 * The Kafka consumer thread calls {@link #broadcast} concurrently with Reactor I/O
 * threads reading from the sink; the sink implementation handles synchronisation internally.
 */
@Component
@Slf4j
public class TickWebSocketHandler implements WebSocketHandler {

    /**
     * Multicast sink — the central message bus for the tick broadcast.
     *
     * <p>All WebSocket sessions subscribe to {@code tickSink.asFlux()}.
     * The Kafka consumer publishes to it via {@link #broadcast(String)}.
     * The sink is stateful and long-lived (exists for the application lifetime).
     */
    private final Sinks.Many<String> tickSink = Sinks.many().multicast().onBackpressureBuffer(256, false);

    /**
     * Publishes a raw tick JSON string to all connected dashboard clients.
     *
     * <p>Called by {@link TickBroadcaster} on every normalised-ticks Kafka message.
     * {@code tryEmitNext} is non-blocking — if the sink is saturated or has no subscribers,
     * the emission is silently dropped rather than throwing. At tick rates of 10–30/sec,
     * this is never a concern in practice.
     *
     * @param tickJson the serialised {@code NormalisedTick} JSON from the Kafka topic
     */
    public void broadcast(final String tickJson) {
        final Sinks.EmitResult result = tickSink.tryEmitNext(tickJson);
        if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            log.warn("tick.broadcast.failed result={}", result);
        }
    }

    /**
     * Handles a new WebSocket connection from the dashboard.
     *
     * <p>Subscribes the session to the multicast tick sink. The returned {@code Mono<Void>}
     * from {@code session.send()} keeps the connection alive for the duration of the sink
     * subscription. When the browser closes the connection (tab close, navigation, or
     * network drop), Reactor-Netty terminates the channel, which cancels the sink
     * subscription and completes the returned Mono.
     *
     * @param session the newly connected WebSocket session
     * @return a Mono that completes when the session is closed
     */
    @Override
    public Mono<Void> handle(final WebSocketSession session) {
        log.debug("ws.session.opened sessionId={}", session.getId());

        // session.receive() must be subscribed alongside session.send() so Reactor-Netty
        // drains the inbound channel. Without this, browser ping frames or any client
        // data back-pressures the inbound buffer, which can force-close the session.
        // firstWithSignal: completes as soon as one side finishes — when the client
        // disconnects, receive.then() completes first and cancels the send subscription.
        final Mono<Void> send = session.send(tickSink.asFlux().map(session::textMessage));
        final Mono<Void> receive = session.receive().then();

        return Mono.firstWithSignal(send, receive)
                .doFinally(signal ->
                        log.debug("ws.session.closed sessionId={} signal={}", session.getId(), signal)
                );
    }
}
