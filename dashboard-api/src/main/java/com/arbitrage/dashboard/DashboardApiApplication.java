package com.arbitrage.dashboard;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the Multi-Asset Cross-Exchange Arbitrage Detection Platform.
 *
 * <p>This Spring Boot application serves as the backend API, providing:
 * <ul>
 *   <li>WebSocket endpoints for real-time price and opportunity feeds ({@code /ws/ticks}, {@code /ws/opportunities})</li>
 *   <li>REST endpoints for analytics and exchange health</li>
 *   <li>Prometheus metrics via Spring Actuator ({@code /actuator/prometheus})</li>
 *   <li>Swagger UI at {@code /swagger-ui.html} for REST API exploration</li>
 * </ul>
 *
 * <p>All other modules (exchange-connectors, normalisation-engine, detection-engine,
 * execution-simulator) are included as dependencies and run within this single JVM
 * during development. In production, they can be split into separate services (see ADR-008).
 *
 * <p>WebSocket endpoints are documented separately in {@code docs/api/WEBSOCKET_API.md}
 * — SpringDoc does not document WebSocket endpoints automatically.
 */
@OpenAPIDefinition(
        info = @Info(
                title = "Multi-Asset Cross-Exchange Arbitrage Detection API",
                version = "1.0",
                description = "REST API for analytics, health, and historical data. "
                        + "WebSocket endpoints (/ws/ticks, /ws/opportunities) are documented "
                        + "in docs/api/WEBSOCKET_API.md."
        )
)
@EnableJpaRepositories(basePackages = "com.arbitrage")
@EntityScan(basePackages = "com.arbitrage")
@SpringBootApplication(scanBasePackages = "com.arbitrage")
public class DashboardApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DashboardApiApplication.class, args);
    }
}
