package com.arbitrage.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Multi-Asset Cross-Exchange Arbitrage Detection Platform.
 *
 * <p>This Spring Boot application serves as the backend API, providing:
 * <ul>
 *   <li>WebSocket endpoints for real-time price, opportunity, and health data push</li>
 *   <li>REST endpoints for historical queries and analytics</li>
 *   <li>Prometheus metrics endpoint via Spring Actuator</li>
 * </ul>
 *
 * <p>All other modules (exchange-connectors, normalisation-engine, detection-engine,
 * execution-simulator) are included as dependencies and run within this single JVM
 * during development. In production, they can be split into separate services.
 */
@SpringBootApplication
public class DashboardApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DashboardApiApplication.class, args);
    }
}
