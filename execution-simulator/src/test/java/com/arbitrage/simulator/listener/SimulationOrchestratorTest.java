package com.arbitrage.simulator.listener;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.entity.SimulationResult;
import com.arbitrage.simulator.model.SlippageResult;
import com.arbitrage.simulator.repository.SimulationResultRepository;
import com.arbitrage.simulator.service.ExecutionTimelineSimulator;
import com.arbitrage.simulator.service.SlippageEstimator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SimulationOrchestrator}.
 *
 * <p>Verifies orchestration logic: lifecycle filtering, disabled-mode short-circuit,
 * net P&amp;L computation, and persistence. All dependencies are Mockito mocks —
 * DB integration is covered separately via Testcontainers (Session 4.6).
 */
@ExtendWith(MockitoExtension.class)
class SimulationOrchestratorTest {

    private static final TradingPair BTC_USDT  = TradingPair.fromSymbol("BTC-USDT");
    private static final long        LATENCY_MS = 43L;

    @Mock private ExecutionTimelineSimulator executionTimelineSimulator;
    @Mock private SlippageEstimator          slippageEstimator;
    @Mock private SimulationResultRepository simulationResultRepository;
    @Mock private Acknowledgment             acknowledgment;

    private SimulationProperties   simulationProperties;
    private SimulationOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        simulationProperties = new SimulationProperties();
        simulationProperties.setEnabled(true);
        simulationProperties.setDetectionToDecisionMs(5L);
        simulationProperties.setDefaultSlippageBps(new BigDecimal("2.00"));
        orchestrator = new SimulationOrchestrator(
                executionTimelineSimulator, slippageEstimator,
                simulationResultRepository, simulationProperties);
    }

    // ─── Lifecycle filtering ──────────────────────────────────────────────────

    @Test
    @DisplayName("CLOSED opportunity is fully simulated and persisted")
    void onOpportunity_closed_simulatesAndPersists() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50100", "0.001", "0.001", "1.0");
        stubSimulation(LATENCY_MS, "50010", "50090", "4.0000");

        orchestrator.onOpportunity(opp, acknowledgment);

        verify(simulationResultRepository).save(any(SimulationResult.class));
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("DETECTED opportunity is skipped without simulation")
    void onOpportunity_detected_skipsSimulation() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.DETECTED, "50000", "50100", "0.001", "0.001", "1.0");

        orchestrator.onOpportunity(opp, acknowledgment);

        verify(simulationResultRepository, never()).save(any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("OPEN opportunity is skipped without simulation")
    void onOpportunity_open_skipsSimulation() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.OPEN, "50000", "50100", "0.001", "0.001", "1.0");

        orchestrator.onOpportunity(opp, acknowledgment);

        verify(simulationResultRepository, never()).save(any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("disabled simulation — all opportunities skipped, offset still acknowledged")
    void onOpportunity_disabled_skipsAllAndAcknowledges() {
        simulationProperties.setEnabled(false);
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50100", "0.001", "0.001", "1.0");

        orchestrator.onOpportunity(opp, acknowledgment);

        verify(simulationResultRepository, never()).save(any());
        verify(acknowledgment).acknowledge();
    }

    // ─── Net P&L calculation ──────────────────────────────────────────────────

    @Test
    @DisplayName("profitable trade — netProfit > 0 and wasProfitable = true")
    void onOpportunity_profitableTrade_setsWasProfitableTrue() {
        // Buy at 50000, sell at 50200, qty=1, fees=0.1% each
        // gross = (50200 - 50000) * 1 = 200
        // buyFee = 50000 * 0.001 * 1 = 50
        // sellFee = 50200 * 0.001 * 1 = 50.2
        // netProfit = 200 - 50 - 50.2 = 99.8 > 0
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50200", "0.001", "0.001", "1.0");
        stubSimulation(LATENCY_MS, "50000", "50200", "0.0000");

        ArgumentCaptor<SimulationResult> captor = ArgumentCaptor.forClass(SimulationResult.class);
        orchestrator.onOpportunity(opp, acknowledgment);
        verify(simulationResultRepository).save(captor.capture());

        SimulationResult saved = captor.getValue();
        assertThat(saved.isWasProfitable()).isTrue();
        assertThat(saved.getNetProfit()).isEqualByComparingTo("99.80000000");
    }

    @Test
    @DisplayName("unprofitable trade after slippage — netProfit < 0 and wasProfitable = false")
    void onOpportunity_unprofitableTrade_setsWasProfitableFalse() {
        // Slippage erodes the spread: buy execution price higher, sell lower
        // Buy at 50100 (slipped up), sell at 50000 (slipped down) — inverted spread
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50100", "0.001", "0.001", "1.0");
        stubSimulation(LATENCY_MS, "50100", "50000", "20.0000"); // heavy slippage

        ArgumentCaptor<SimulationResult> captor = ArgumentCaptor.forClass(SimulationResult.class);
        orchestrator.onOpportunity(opp, acknowledgment);
        verify(simulationResultRepository).save(captor.capture());

        assertThat(captor.getValue().isWasProfitable()).isFalse();
        assertThat(captor.getValue().getNetProfit()).isLessThan(BigDecimal.ZERO);
    }

    // ─── Persisted fields ─────────────────────────────────────────────────────

    @Test
    @DisplayName("persisted SimulationResult has correct metadata fields")
    void onOpportunity_closed_persistsCorrectMetadata() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50100", "0.001", "0.001", "1.0");
        stubSimulation(LATENCY_MS, "50010", "50090", "4.0000");

        ArgumentCaptor<SimulationResult> captor = ArgumentCaptor.forClass(SimulationResult.class);
        orchestrator.onOpportunity(opp, acknowledgment);
        verify(simulationResultRepository).save(captor.capture());

        SimulationResult saved = captor.getValue();
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getOpportunityId()).isEqualTo(opp.getId());
        assertThat(saved.getSimulatedLatencyMs()).isEqualTo(LATENCY_MS);
        assertThat(saved.getSlippageBps()).isEqualByComparingTo("4.0000");
        assertThat(saved.getSimulatedBuyPrice()).isEqualByComparingTo("50010");
        assertThat(saved.getSimulatedSellPrice()).isEqualByComparingTo("50090");
        assertThat(saved.getSimulatedQuantity()).isEqualByComparingTo("1.0");
        assertThat(saved.getFillProbability()).isEqualByComparingTo("1.0000");
        assertThat(saved.getSimulationTimestamp()).isNotNull();
    }

    @Test
    @DisplayName("offset is always acknowledged even if repository throws")
    void onOpportunity_repositoryThrows_offsetStillAcknowledged() {
        ArbitrageOpportunity opp = buildOpportunity(OpportunityStatus.CLOSED, "50000", "50100", "0.001", "0.001", "1.0");
        stubSimulation(LATENCY_MS, "50010", "50090", "4.0000");
        when(simulationResultRepository.save(any())).thenThrow(new RuntimeException("DB unavailable"));

        orchestrator.onOpportunity(opp, acknowledgment);

        verify(acknowledgment).acknowledge();
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private ArbitrageOpportunity buildOpportunity(OpportunityStatus status,
                                                   String buyPrice, String sellPrice,
                                                   String buyFeeRate, String sellFeeRate,
                                                   String quantity) {
        BigDecimal buy  = new BigDecimal(buyPrice);
        BigDecimal sell = new BigDecimal(sellPrice);
        BigDecimal qty  = new BigDecimal(quantity);
        return ArbitrageOpportunity.builder()
                .id(UUID.randomUUID())
                .tradingPair(BTC_USDT)
                .buyExchange(ExchangeId.BINANCE)
                .sellExchange(ExchangeId.BYBIT)
                .buyPrice(buy)
                .buyQuantity(qty)
                .buyFeeRate(new BigDecimal(buyFeeRate))
                .sellPrice(sell)
                .sellQuantity(qty)
                .sellFeeRate(new BigDecimal(sellFeeRate))
                .grossSpread(sell.subtract(buy))
                .netSpread(sell.subtract(buy))
                .grossSpreadBps(BigDecimal.TEN)
                .netSpreadBps(BigDecimal.ONE)
                .arbitrageableQuantity(qty)
                .theoreticalProfit(sell.subtract(buy).multiply(qty))
                .status(status)
                .detectionTimestamp(Instant.now().minusMillis(200))
                .lastUpdateTimestamp(Instant.now())
                .detectedNanoTime(System.nanoTime())
                .closedNanoTime(System.nanoTime())
                .peakNetSpread(sell.subtract(buy))
                .averageNetSpread(sell.subtract(buy))
                .totalDurationMs(100L)
                .updateCount(3L)
                .build();
    }

    private void stubSimulation(long latencyMs, String buyExecPrice,
                                String sellExecPrice, String totalSlippageBps) {
        when(executionTimelineSimulator.simulateExecutionTimeMs(any(), any(), anyLong()))
                .thenReturn(latencyMs);
        SlippageResult slippage = SlippageResult.builder()
                .buyExecutionPrice(new BigDecimal(buyExecPrice))
                .sellExecutionPrice(new BigDecimal(sellExecPrice))
                .buySlippageBps(new BigDecimal(totalSlippageBps).divide(new BigDecimal("2"), 4, java.math.RoundingMode.HALF_UP))
                .sellSlippageBps(new BigDecimal(totalSlippageBps).divide(new BigDecimal("2"), 4, java.math.RoundingMode.HALF_UP))
                .totalSlippageBps(new BigDecimal(totalSlippageBps))
                .priceDataAvailable(true)
                .build();
        when(slippageEstimator.estimate(any(), anyLong())).thenReturn(slippage);
    }
}
