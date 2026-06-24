package com.arbitrage.normalisation;

import com.arbitrage.normalisation.config.NormalisationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module entry point for the Normalisation Engine.
 *
 * <p>This is a library module (bootJar disabled) — it does not run standalone.
 * Its beans are component-scanned by {@code DashboardApiApplication} via
 * {@code @SpringBootApplication(scanBasePackages = "com.arbitrage")}.
 *
 * <p><b>Responsibility:</b> The normalisation engine is the "feed handler normaliser" in
 * HFT terminology. It consumes raw exchange-specific tick data from three Kafka topics
 * ({@code raw-ticks-binance}, {@code raw-ticks-bybit}, {@code raw-ticks-kucoin}),
 * transforms each exchange's wire format into a unified {@link com.arbitrage.common.model.NormalisedTick},
 * and produces to the {@code normalised-ticks} topic consumed by downstream stages.
 *
 * <p><b>Why isolation matters:</b> All exchange-specific quirks (symbol format differences,
 * timestamp precision, nested JSON structures) are absorbed here. The detection engine
 * downstream sees only {@code NormalisedTick} — it has zero knowledge of which exchange
 * produced the data or what the wire format looked like.
 *
 * <p><b>Why {@code @EnableScheduling} here?</b> {@code @Scheduled} on {@link
 * com.arbitrage.normalisation.service.FeedHealthMonitor#checkFeedHealth()} requires
 * the scheduling infrastructure to be activated. Since this module is scanned by
 * {@code dashboard-api}'s {@code @SpringBootApplication}, placing {@code @EnableScheduling}
 * here (on our own {@code @Configuration}) is cleaner than polluting the dashboard-api
 * entry point with a concern that belongs to the normalisation engine.
 */
@Configuration
@ComponentScan(basePackages = "com.arbitrage.normalisation")
@EnableScheduling
@EnableConfigurationProperties(NormalisationProperties.class)
public class NormalisationEngineApplication {
    // Module marker — beans registered via component scan
}
