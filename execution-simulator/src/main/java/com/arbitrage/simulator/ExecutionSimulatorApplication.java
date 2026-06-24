package com.arbitrage.simulator;

import com.arbitrage.simulator.config.LatencyConfiguration;
import com.arbitrage.simulator.config.SimulationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module entry point for the Execution Simulator.
 *
 * <p>This is a library module (bootJar disabled) — it does not run standalone.
 * Its beans are component-scanned by {@code DashboardApiApplication} via
 * {@code @SpringBootApplication(scanBasePackages = "com.arbitrage")}.
 *
 * <p><b>Responsibility:</b> The execution simulator answers the question detection
 * alone cannot: "Could you have actually captured this opportunity?" For every
 * CLOSED {@link com.arbitrage.common.model.ArbitrageOpportunity}, it models:
 * <ul>
 *   <li>Total execution time (network + exchange processing, both legs in parallel)</li>
 *   <li>Price movement during that execution window (replay-based, not Monte Carlo)</li>
 *   <li>Net profit after fees and slippage at the simulated execution price</li>
 * </ul>
 * Results are persisted to the {@code simulation_results} TimescaleDB table.
 *
 * <p><b>Why {@code @Configuration} not {@code @SpringBootApplication}?</b>
 * Mirrors the detection-engine pattern. This module is a library with no standalone
 * main method. {@code @Configuration} with {@code @ComponentScan} registers its beans
 * into the parent {@code dashboard-api} application context without triggering a
 * redundant Boot startup sequence.
 *
 * <p><b>Why {@code @EnableScheduling}?</b> Session 4.6 adds a {@code @Scheduled}
 * method for daily P&amp;L aggregation. Enabling it here (on the module's own
 * {@code @Configuration}) avoids polluting the dashboard-api entry point with
 * simulation-specific concerns.
 */
@Configuration
@ComponentScan(basePackages = "com.arbitrage.simulator")
@EnableScheduling
@EnableConfigurationProperties({SimulationProperties.class, LatencyConfiguration.class})
public class ExecutionSimulatorApplication {
    // Module marker — beans registered via component scan
}
