package com.arbitrage.dashboard.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.config.CorsRegistry;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * WebFlux configuration for the dashboard API server.
 *
 * <p><strong>Why WebFlux, not MVC?</strong> This application serves real-time WebSocket
 * connections to the dashboard UI. Spring MVC uses the servlet model — one thread per
 * connection. With 3 exchange feeds + hundreds of dashboard clients, thread count
 * explodes (each thread costs ~1MB of stack memory). WebFlux uses non-blocking I/O with
 * a small fixed thread pool (typically equal to CPU core count), allowing a single thread
 * to handle thousands of concurrent connections without blocking.
 *
 * <p><strong>CORS (Cross-Origin Resource Sharing):</strong> The React dashboard runs on
 * {@code localhost:5173} (Vite dev server) while this API runs on {@code localhost:8080}.
 * Browsers enforce the Same-Origin Policy — requests from one origin to another are
 * blocked by default. CORS headers tell the browser that cross-origin requests from
 * the dashboard are allowed.
 *
 * <p>In production, the CORS allowed origins would be restricted to the actual dashboard
 * domain. In development, we allow {@code localhost:5173} and {@code localhost:3000}
 * (common dev server ports).
 *
 * @see org.springframework.web.reactive.config.WebFluxConfigurer
 */
@Configuration
@EnableWebFlux
public class WebFluxConfig implements WebFluxConfigurer {

    /**
     * Configures CORS mappings to allow the React dashboard to make API calls.
     *
     * <p>Without this, the browser would block every fetch/WebSocket request from
     * the dashboard to the API because they're on different ports (different origins).
     *
     * <p><strong>Why {@code allowedOriginPatterns} instead of {@code allowedOrigins}?</strong>
     * Vite auto-increments the port (5173 → 5174 → 5175 …) when the default port is taken
     * by another process. Hardcoding {@code localhost:5173} breaks WebSocket connections when
     * Vite lands on a different port — the WebSocket upgrade request carries the browser's
     * {@code Origin} header, which Spring validates against the CORS allow-list.
     * {@code allowedOriginPatterns} supports {@code *} wildcards and — unlike
     * {@code allowedOrigins("*")} — is compatible with {@code allowCredentials(true)}.
     *
     * <p><strong>Security note:</strong> In production, replace the localhost pattern with
     * the actual dashboard domain. Never use a wildcard in production.
     *
     * @param registry the CORS registry to configure
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("http://localhost:*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }
}
