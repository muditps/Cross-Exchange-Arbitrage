package com.arbitrage.detection.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DetectionProperties}.
 *
 * <p>Verifies that default field values are correct and that the class is
 * mutable (required for Spring's {@code @ConfigurationProperties} setter-based binding).
 * Spring's relaxed binding (kebab-case YAML → camelCase fields) is framework-tested;
 * we focus on our defaults, mutability, and the annotation prefix.
 */
class DetectionPropertiesTest {

    @Test
    @DisplayName("Default staleness threshold is 500ms")
    void defaultStalenessThresholdMs_is500() {
        assertThat(new DetectionProperties().getStalenessThresholdMs()).isEqualTo(500L);
    }

    @Test
    @DisplayName("Default min spread is 10 bps")
    void defaultMinSpreadBps_is10() {
        assertThat(new DetectionProperties().getMinSpreadBps()).isEqualTo(10);
    }

    @Test
    @DisplayName("Default Redis price TTL is 10000ms (10 seconds)")
    void defaultRedisPriceTtlMs_is10000() {
        assertThat(new DetectionProperties().getRedisPriceTtlMs()).isEqualTo(10_000L);
    }

    @Test
    @DisplayName("Redis TTL default is larger than staleness threshold — cleanup backstop, not comparison filter")
    void redisPriceTtlMs_isLargerThan_stalenessThresholdMs() {
        DetectionProperties props = new DetectionProperties();
        assertThat(props.getRedisPriceTtlMs()).isGreaterThan(props.getStalenessThresholdMs());
    }

    @Test
    @DisplayName("All three properties can be overridden via setters")
    void setters_overrideDefaults() {
        DetectionProperties props = new DetectionProperties();
        props.setStalenessThresholdMs(250L);
        props.setMinSpreadBps(5);
        props.setRedisPriceTtlMs(5_000L);

        assertThat(props.getStalenessThresholdMs()).isEqualTo(250L);
        assertThat(props.getMinSpreadBps()).isEqualTo(5);
        assertThat(props.getRedisPriceTtlMs()).isEqualTo(5_000L);
    }

    @Test
    @DisplayName("ConfigurationProperties annotation has the correct prefix")
    void configurationPropertiesAnnotation_hasCorrectPrefix() {
        ConfigurationProperties annotation =
                DetectionProperties.class.getAnnotation(ConfigurationProperties.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.prefix()).isEqualTo("arbitrage.detection");
    }
}
