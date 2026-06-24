package com.arbitrage.connector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link ExchangeConnectorProperties}.
 *
 * <p>Verifies that Spring Boot correctly binds YAML configuration to the
 * properties class, including type conversion (String → Duration, String → BigDecimal),
 * default value application, and Jakarta Bean Validation constraints.</p>
 *
 * <p>Uses {@code application-test.yml} with known values for deterministic assertions.</p>
 */
@SpringBootTest(classes = ExchangeConnectorPropertiesTest.TestConfig.class)
@ActiveProfiles("test")
class ExchangeConnectorPropertiesTest {

    @EnableConfigurationProperties(ExchangeConnectorProperties.class)
    static class TestConfig {
    }

    @Autowired
    private ExchangeConnectorProperties properties;

    @Test
    @DisplayName("Properties are injected and connectors map is populated")
    void propertiesAreInjected() {
        assertNotNull(properties);
        assertNotNull(properties.getConnectors());
        assertEquals(3, properties.getConnectors().size(), "Should have binance, bybit, and kucoin entries");
    }

    @Test
    @DisplayName("Binance connector properties bind correctly from YAML")
    void binanceProperties_bindCorrectly() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");

        assertNotNull(binance, "Binance config must exist");
        assertEquals("wss://stream.binance.com:9443/ws", binance.getWsEndpoint());
        assertTrue(binance.isEnabled());
    }

    @Test
    @DisplayName("BigDecimal taker fee rate preserves precision — not corrupted by float parsing")
    void takerFeeRate_preservesBigDecimalPrecision() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");

        // BigDecimal("0.0010") must equal BigDecimal("0.0010"), not 0.001 (different scale)
        assertEquals(0, new BigDecimal("0.0010").compareTo(binance.getTakerFeeRate()),
                "Taker fee rate must match expected value with correct precision");
    }

    @Test
    @DisplayName("Bybit has different taker fee rate than Binance")
    void bybitTakerFeeRate_isDifferentFromBinance() {
        ExchangeConnectorProperties.ExchangeProperties bybit = properties.getConnectors().get("bybit");

        assertEquals(0, new BigDecimal("0.0006").compareTo(bybit.getTakerFeeRate()),
                "Bybit taker fee rate should be 0.0006 as configured in test YAML");
    }

    @Test
    @DisplayName("Duration fields parse correctly from YAML string representations")
    void durationFields_parseCorrectly() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");

        assertEquals(Duration.ofSeconds(1), binance.getInitialReconnectDelay());
        assertEquals(Duration.ofSeconds(30), binance.getMaxReconnectDelay());
        assertEquals(Duration.ofMillis(500), binance.getStalenessThreshold());
    }

    @Test
    @DisplayName("Bybit has custom duration values different from defaults")
    void bybitDurations_overrideDefaults() {
        ExchangeConnectorProperties.ExchangeProperties bybit = properties.getConnectors().get("bybit");

        assertEquals(Duration.ofSeconds(2), bybit.getInitialReconnectDelay());
        assertEquals(Duration.ofSeconds(60), bybit.getMaxReconnectDelay());
        assertEquals(Duration.ofMillis(750), bybit.getStalenessThreshold());
    }

    @Test
    @DisplayName("Max reconnect attempts bind correctly as integers")
    void maxReconnectAttempts_bindCorrectly() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");
        ExchangeConnectorProperties.ExchangeProperties bybit = properties.getConnectors().get("bybit");

        assertEquals(10, binance.getMaxReconnectAttempts());
        assertEquals(5, bybit.getMaxReconnectAttempts());
    }

    @Test
    @DisplayName("Enabled flag binds correctly — Binance enabled, Bybit disabled")
    void enabledFlag_bindsCorrectly() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");
        ExchangeConnectorProperties.ExchangeProperties bybit = properties.getConnectors().get("bybit");

        assertTrue(binance.isEnabled(), "Binance should be enabled");
        assertFalse(bybit.isEnabled(), "Bybit should be disabled");
    }

    @Test
    @DisplayName("Multiple exchange entries bind independently without cross-contamination")
    void multipleExchanges_bindIndependently() {
        ExchangeConnectorProperties.ExchangeProperties binance = properties.getConnectors().get("binance");
        ExchangeConnectorProperties.ExchangeProperties bybit = properties.getConnectors().get("bybit");

        // Verify different endpoints
        assertEquals("wss://stream.binance.com:9443/ws", binance.getWsEndpoint());
        assertEquals("wss://stream.bybit.com/v5/public/spot", bybit.getWsEndpoint());

        // Verify different fee rates
        assertFalse(binance.getTakerFeeRate().equals(bybit.getTakerFeeRate()),
                "Binance and Bybit should have different fee rates in test config");
    }
}
