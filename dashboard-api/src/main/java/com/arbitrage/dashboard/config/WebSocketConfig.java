package com.arbitrage.dashboard.config;

import com.arbitrage.dashboard.websocket.OpportunityWebSocketHandler;
import com.arbitrage.dashboard.websocket.TickWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.Map;

/**
 * Registers WebSocket endpoint routes for the dashboard API.
 *
 * <p><strong>Why two beans?</strong> Spring WebFlux WebSocket requires both:
 * <ol>
 *   <li>{@link HandlerMapping} — maps URL patterns to handler instances.
 *       Uses {@link SimpleUrlHandlerMapping#setUrlMap} (reactive WebFlux API —
 *       note: NOT {@code setHandlerMap}, which is the Spring MVC API on the same class name
 *       but a different package).</li>
 *   <li>{@link WebSocketHandlerAdapter} — tells Spring WebFlux how to invoke a
 *       {@link WebSocketHandler}. Without this bean, Spring would not know how to
 *       delegate a matched WebSocket upgrade request to the handler. This is different
 *       from Spring MVC, where the adapter is registered automatically. In WebFlux,
 *       the WebSocket adapter must be explicitly declared.</li>
 * </ol>
 *
 * <p><strong>Order -1:</strong> Spring WebFlux handler mappings are ordered. The default
 * annotation-based mapping ({@code RequestMappingHandlerMapping}) has order 0. Setting
 * order -1 ensures the WebSocket mapping is checked first, preventing annotated
 * {@code @GetMapping("/ws/...")} controllers from accidentally intercepting WebSocket
 * upgrade requests.
 *
 * <p><strong>URL scheme:</strong> Both {@code /ws/ticks} and {@code /ws/opportunities} are
 * proxied by the Vite dev server under {@code /ws/*} → {@code ws://localhost:8080}.
 * In production, the nginx/load-balancer forwards {@code wss://} upgrade requests.
 *
 * <p><strong>Why no {@code HealthWebSocketHandler}?</strong> The original plan (DEVELOPMENT_PLAN.md
 * Session 5.3) listed a health WebSocket endpoint. It was intentionally replaced with REST polling
 * in the dashboard-api implementation. Health state changes slowly (CONNECTED/STALE transitions
 * happen at 5-second intervals) — a persistent WebSocket connection is wasteful when REST polling
 * every 5s is functionally equivalent and simpler to implement, test, and debug. The frontend uses
 * TanStack Query for health polling ({@code useExchangeHealth} at 5s, {@code useSystemHealth} at 10s).
 * WebSocket is appropriate for data where every event matters and latency is perceptible; health
 * status does not meet that bar.
 */
@Configuration
public class WebSocketConfig {

    /**
     * Maps WebSocket endpoint URLs to their handler implementations.
     *
     * <p>Current endpoints:
     * <ul>
     *   <li>{@code /ws/ticks} — real-time normalised tick stream (best bid/ask per exchange)</li>
     *   <li>{@code /ws/opportunities} — real-time arbitrage opportunity lifecycle events
     *       (DETECTED, OPEN, CLOSED, EXPIRED)</li>
     * </ul>
     *
     * @param tickHandler        broadcasts normalised ticks to connected clients
     * @param opportunityHandler broadcasts opportunity lifecycle events to connected clients
     * @return a URL-to-handler mapping with priority order -1
     */
    @Bean
    public HandlerMapping webSocketHandlerMapping(
            final TickWebSocketHandler tickHandler,
            final OpportunityWebSocketHandler opportunityHandler) {
        final Map<String, WebSocketHandler> urlMap = Map.of(
                "/ws/ticks", tickHandler,
                "/ws/opportunities", opportunityHandler
        );

        final SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(urlMap);
        mapping.setOrder(-1);
        return mapping;
    }

    /**
     * Registers the adapter that bridges Spring WebFlux's handler invocation pipeline
     * to the {@link WebSocketHandler} contract.
     *
     * <p>Without this bean, Spring WebFlux would match the URL via the mapping above
     * but then fail with "No handler found" because no adapter is registered for the
     * {@link WebSocketHandler} type.
     *
     * @return the WebSocket handler adapter
     */
    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
