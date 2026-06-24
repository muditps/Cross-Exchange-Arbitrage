package com.arbitrage.connector.config;

import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.TradingPairsProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Spring auto-configuration for the exchange connectors module.
 *
 * <p>Registers {@link ExchangeConnectorProperties} and {@link TradingPairsProperties}
 * as Spring beans via {@link EnableConfigurationProperties}. This ensures both
 * property classes are bound from {@code application.yml} and available for
 * injection across the module without requiring {@code @ConfigurationPropertiesScan}
 * on the application entry point.</p>
 *
 * <p><b>Why a separate config class?</b> {@link EnableConfigurationProperties} is
 * idempotent and can live anywhere in the component scan. Centralising it here
 * means only one place to update when adding new {@code @ConfigurationProperties}
 * classes to this module — the entry points (test configs, dashboard-api) do not
 * need to be updated.</p>
 *
 * <p><b>What this class does NOT do:</b> It does not define any {@code @Bean} methods.
 * The actual registry bean is {@link com.arbitrage.connector.registry.ExchangeConnectorRegistry}
 * (annotated {@code @Component}) and the producers are also {@code @Component} — Spring
 * picks them up via component scan. This class's sole job is properties registration.</p>
 *
 * @see ExchangeConnectorProperties for per-exchange connection settings
 * @see TradingPairsProperties for the list of trading pairs to monitor
 */
@Configuration
@EnableConfigurationProperties({ExchangeConnectorProperties.class, TradingPairsProperties.class})
public class ConnectorAutoConfiguration {
}
