package com.arbitrage.simulator.model;

import com.arbitrage.common.model.ExchangeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangeLatencyProfileTest {

    // ─── totalLegLatencyMs ────────────────────────────────────────────────────

    @Test
    @DisplayName("totalLegLatencyMs returns sum of three deterministic components (excludes jitter)")
    void totalLegLatencyMs_sumsThreeDeterministicComponents() {
        ExchangeLatencyProfile profile = ExchangeLatencyProfile.builder()
                .exchangeId(ExchangeId.BINANCE)
                .networkLatencyMs(15)
                .exchangeProcessingMs(5)
                .confirmationLatencyMs(5)
                .jitterMs(2)
                .build();

        // jitterMs=2 is NOT included — jitter is sampled randomly in ExecutionTimelineSimulator
        assertThat(profile.totalLegLatencyMs()).isEqualTo(25L);
    }

    @Test
    @DisplayName("totalLegLatencyMs returns 0 when all components are 0")
    void totalLegLatencyMs_withAllZeros_returnsZero() {
        ExchangeLatencyProfile profile = ExchangeLatencyProfile.builder()
                .exchangeId(ExchangeId.BYBIT)
                .networkLatencyMs(0)
                .exchangeProcessingMs(0)
                .confirmationLatencyMs(0)
                .jitterMs(0)
                .build();

        assertThat(profile.totalLegLatencyMs()).isEqualTo(0L);
    }

    @Test
    @DisplayName("totalLegLatencyMs with only network component set returns that value")
    void totalLegLatencyMs_onlyNetworkSet_returnsNetworkMs() {
        ExchangeLatencyProfile profile = ExchangeLatencyProfile.builder()
                .exchangeId(ExchangeId.KUCOIN)
                .networkLatencyMs(30)
                .exchangeProcessingMs(0)
                .confirmationLatencyMs(0)
                .jitterMs(0)
                .build();

        assertThat(profile.totalLegLatencyMs()).isEqualTo(30L);
    }

    @Test
    @DisplayName("totalLegLatencyMs with Bybit defaults returns 41ms (excluding 2ms jitter)")
    void totalLegLatencyMs_withBybitDefaults_returns41ms() {
        ExchangeLatencyProfile profile = ExchangeLatencyProfile.builder()
                .exchangeId(ExchangeId.BYBIT)
                .networkLatencyMs(25)
                .exchangeProcessingMs(8)
                .confirmationLatencyMs(8)
                .jitterMs(2)
                .build();

        assertThat(profile.totalLegLatencyMs()).isEqualTo(41L); // 25+8+8, not +2
    }

    @Test
    @DisplayName("totalLegLatencyMs with KuCoin defaults returns 50ms (excluding 2ms jitter)")
    void totalLegLatencyMs_withKucoinDefaults_returns50ms() {
        ExchangeLatencyProfile profile = ExchangeLatencyProfile.builder()
                .exchangeId(ExchangeId.KUCOIN)
                .networkLatencyMs(30)
                .exchangeProcessingMs(10)
                .confirmationLatencyMs(10)
                .jitterMs(2)
                .build();

        assertThat(profile.totalLegLatencyMs()).isEqualTo(50L); // 30+10+10, not +2
    }
}
