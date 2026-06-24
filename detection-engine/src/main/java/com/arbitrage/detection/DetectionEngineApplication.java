package com.arbitrage.detection;

import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.config.FeeConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module entry point for the Detection Engine.
 *
 * <p>This is a library module (bootJar disabled) — it does not run standalone.
 * Its beans are component-scanned by {@code DashboardApiApplication} via
 * {@code @SpringBootApplication(scanBasePackages = "com.arbitrage")}.
 *
 * <p><b>Responsibility:</b> The detection engine is the core business logic layer.
 * It consumes unified {@link com.arbitrage.common.model.NormalisedTick} messages from
 * the {@code normalised-ticks} Kafka topic, maintains per-exchange price state in Redis,
 * and compares prices across all exchanges to detect arbitrage opportunities that exceed
 * the net transaction cost (exchange fees + slippage estimate).
 *
 * <p><b>Why {@code @Configuration} not {@code @SpringBootApplication}?</b>
 * This module is a library — it has no standalone main class. {@code @SpringBootApplication}
 * would trigger a full Boot startup sequence (auto-configuration, embedded server) which is
 * neither correct nor needed. {@code @Configuration} with {@code @ComponentScan} registers
 * this module's beans into the parent {@code dashboard-api} application context.
 *
 * <p><b>Why {@code @EnableScheduling} here?</b> Future sessions will add
 * {@code @Scheduled} methods for stale-opportunity expiry and feed health cross-checks.
 * Enabling scheduling here (on our own {@code @Configuration}) is cleaner than adding
 * it to the dashboard-api entry point, which should not need to know about detection concerns.
 */
@Configuration
@ComponentScan(basePackages = "com.arbitrage.detection")
@EnableScheduling
@EnableConfigurationProperties({DetectionProperties.class, FeeConfiguration.class})
public class DetectionEngineApplication {
    // Module marker — beans registered via component scan
}
