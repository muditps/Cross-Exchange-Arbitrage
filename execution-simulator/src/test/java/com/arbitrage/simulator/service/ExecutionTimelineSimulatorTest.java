package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.simulator.config.LatencyConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ExecutionTimelineSimulator}.
 *
 * <p>Uses a directly-instantiated {@link LatencyConfiguration} with jitter zeroed out
 * (Binance=25ms, Bybit=41ms, KuCoin=50ms base) — no Spring context needed. This keeps
 * tests deterministic and focused on the {@code max(legA, legB)} formula.
 */
class ExecutionTimelineSimulatorTest {

    // Base leg totals with jitterMs=0 (deterministic for tests):
    // Binance: 15+5+5 = 25ms
    // Bybit:   25+8+8 = 41ms
    // KuCoin:  30+10+10 = 50ms
    //
    // Production uses non-zero jitter (15–20ms) sampled randomly per leg,
    // producing a latency range rather than fixed values. Tests zero it out
    // to keep assertions deterministic.

    private ExecutionTimelineSimulator simulator;

    @BeforeEach
    void setUp() {
        LatencyConfiguration config = new LatencyConfiguration();
        config.getBinance().setJitterMs(0);
        config.getBybit().setJitterMs(0);
        config.getKucoin().setJitterMs(0);
        simulator = new ExecutionTimelineSimulator(config);
    }

    // ─── Core formula: max(legA, legB) ────────────────────────────────────────

    @Test
    @DisplayName("Binance buy + Bybit sell: uses slower Bybit leg (41ms), not sum (66ms)")
    void simulate_binanceBuy_bybitSell_usesSlowerBybitLeg() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BINANCE, ExchangeId.BYBIT, 0);

        assertThat(result).isEqualTo(41L); // max(25, 41) = 41, not 25+41=66
    }

    @Test
    @DisplayName("Bybit buy + Binance sell: still uses slower Bybit leg regardless of buy/sell direction")
    void simulate_bybitBuy_binanceSell_usesSlowerBybitLeg() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BYBIT, ExchangeId.BINANCE, 0);

        assertThat(result).isEqualTo(41L); // max(41, 25) = 41 — direction doesn't change the max
    }

    @Test
    @DisplayName("Binance buy + KuCoin sell: uses slower KuCoin leg (50ms)")
    void simulate_binanceBuy_kucoinSell_usesSlowerKucoinLeg() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BINANCE, ExchangeId.KUCOIN, 0);

        assertThat(result).isEqualTo(50L);
    }

    @Test
    @DisplayName("Bybit buy + KuCoin sell: uses slower KuCoin leg (50ms)")
    void simulate_bybitBuy_kucoinSell_usesSlowerKucoinLeg() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BYBIT, ExchangeId.KUCOIN, 0);

        assertThat(result).isEqualTo(50L);
    }

    @Test
    @DisplayName("Same exchange both sides: uses that exchange's leg total once")
    void simulate_sameExchangeBothSides_usesSingleLegTotal() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BINANCE, ExchangeId.BINANCE, 0);

        assertThat(result).isEqualTo(25L); // max(25, 25) = 25
    }

    // ─── detectionToDecisionMs is additive ────────────────────────────────────

    @Test
    @DisplayName("detectionToDecisionMs is added to max leg total")
    void simulate_addsDetectionDecisionTimeToMaxLeg() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BINANCE, ExchangeId.BYBIT, 10);

        assertThat(result).isEqualTo(51L); // 10 + max(25, 41) = 10 + 41
    }

    @Test
    @DisplayName("Zero detectionToDecisionMs returns only max leg total")
    void simulate_zeroDecisionTime_returnsMaxLegOnly() {
        long result = simulator.simulateExecutionTimeMs(ExchangeId.BYBIT, ExchangeId.KUCOIN, 0);

        assertThat(result).isEqualTo(50L); // 0 + max(41, 50)
    }

    // ─── Custom latency profiles ───────────────────────────────────────────────

    @Test
    @DisplayName("Custom latencies: buy leg slower than sell leg, uses buy leg")
    void simulate_customLatencies_usesBuyLegWhenSlower() {
        LatencyConfiguration config = new LatencyConfiguration();
        // Override Binance to be very slow (buy). Bybit uses default jitter (0-2ms, base 41ms, sell).
        config.getBinance().setNetworkLatencyMs(100);
        config.getBinance().setExchangeProcessingMs(0);
        config.getBinance().setConfirmationLatencyMs(0);
        config.getBinance().setJitterMs(0);

        ExecutionTimelineSimulator customSimulator = new ExecutionTimelineSimulator(config);
        long result = customSimulator.simulateExecutionTimeMs(ExchangeId.BINANCE, ExchangeId.BYBIT, 5);

        assertThat(result).isEqualTo(105L); // 5 + max(100, 41-43) = 5 + 100
    }
}
