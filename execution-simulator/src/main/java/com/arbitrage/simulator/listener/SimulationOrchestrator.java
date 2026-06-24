package com.arbitrage.simulator.listener;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.OpportunityStatus;
import com.arbitrage.simulator.config.SimulationKafkaConsumerConfig;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.entity.SimulationResult;
import com.arbitrage.simulator.model.SlippageResult;
import com.arbitrage.simulator.repository.SimulationResultRepository;
import com.arbitrage.simulator.service.ExecutionTimelineSimulator;
import com.arbitrage.simulator.service.SlippageEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;

/**
 * Kafka listener that orchestrates the full simulation pipeline for closed opportunities.
 *
 * <p><b>Pipeline (per message):</b>
 * <ol>
 *   <li>Receive {@link ArbitrageOpportunity} from {@code arbitrage-opportunities} topic</li>
 *   <li>Skip if not {@code CLOSED} — only simulate completed lifecycle events</li>
 *   <li>Skip if simulation is disabled (test/dev environments without TimescaleDB)</li>
 *   <li>Compute execution time via {@link ExecutionTimelineSimulator}</li>
 *   <li>Estimate slippage via {@link SlippageEstimator}</li>
 *   <li>Compute net P&amp;L</li>
 *   <li>Persist {@link SimulationResult} to TimescaleDB</li>
 *   <li>Acknowledge Kafka offset (MANUAL_IMMEDIATE)</li>
 * </ol>
 *
 * <p><b>Why only CLOSED opportunities?</b>
 * DETECTED events are opening events — the opportunity may close immediately or persist
 * for seconds. Simulating at detection time would produce meaningless results: we'd
 * simulate before knowing how long the opportunity lasted or what its peak spread was.
 * OPEN events are intermediate updates — same problem. EXPIRED events represent data
 * artefacts (feed reconnects) rather than real opportunities. CLOSED is the only state
 * where the full lifecycle is known.
 *
 * <p><b>Why MANUAL_IMMEDIATE ack?</b>
 * The Kafka offset is committed only after the TimescaleDB write succeeds. If the write
 * fails (DB unavailable, constraint violation), the offset is NOT committed and the message
 * is reprocessed on the next poll. This prevents silent simulation gaps — at-least-once
 * delivery for simulation records.
 *
 * <p><b>Net P&amp;L formula:</b>
 * {@code (sellPrice − buyPrice) × quantity − (buyPrice × buyFeeRate × quantity)
 *         − (sellPrice × sellFeeRate × quantity)}.
 * Uses execution prices (from slippage estimator), not detection prices.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SimulationOrchestrator {

    private static final BigDecimal FULL_FILL_PROBABILITY = new BigDecimal("1.0000");
    private static final int PROFIT_SCALE = 8;

    private final ExecutionTimelineSimulator executionTimelineSimulator;
    private final SlippageEstimator slippageEstimator;
    private final SimulationResultRepository simulationResultRepository;
    private final SimulationProperties simulationProperties;

    /**
     * Receives closed opportunity events and drives the full simulation pipeline.
     *
     * <p>Non-CLOSED events are acknowledged immediately without simulation.
     * When simulation is disabled ({@code arbitrage.simulation.enabled=false}),
     * all events are acknowledged immediately — used in test environments without TimescaleDB.
     *
     * @param opportunity   the opportunity event consumed from Kafka
     * @param acknowledgment manual Kafka offset acknowledgment — always called, even on skip
     */
    @KafkaListener(
            topics = SimulationKafkaConsumerConfig.TOPIC_ARBITRAGE_OPPORTUNITIES,
            groupId = SimulationKafkaConsumerConfig.CONSUMER_GROUP,
            containerFactory = "simulationListenerContainerFactory"
    )
    public void onOpportunity(ArbitrageOpportunity opportunity, Acknowledgment acknowledgment) {
        try {
            if (!simulationProperties.isEnabled()) {
                log.debug("Simulation disabled — skipping opportunity={}", opportunity.getId());
                return;
            }

            if (opportunity.getStatus() != OpportunityStatus.CLOSED) {
                log.debug("Skipping non-CLOSED opportunity={} status={}",
                        opportunity.getId(), opportunity.getStatus());
                return;
            }

            try {
                simulate(opportunity);
            } catch (Exception e) {
                // Log and swallow — always advance the offset so the consumer doesn't stall.
                // A persistent DB failure will produce repeated log errors; the correct
                // production remedy is a dead letter queue (DLQ), not infinite retry on
                // the same message which would block all subsequent opportunities.
                log.error("Simulation failed for opportunityId={}: {}",
                        opportunity.getId(), e.getMessage(), e);
            }

        } finally {
            // Always acknowledge — even on skip or exception — to prevent consumer lag.
            acknowledgment.acknowledge();
        }
    }

    private void simulate(ArbitrageOpportunity opportunity) {
        long executionLatencyMs = executionTimelineSimulator.simulateExecutionTimeMs(
                opportunity.getBuyExchange(),
                opportunity.getSellExchange(),
                simulationProperties.getDetectionToDecisionMs());

        SlippageResult slippage = slippageEstimator.estimate(opportunity, executionLatencyMs);

        BigDecimal quantity    = opportunity.getArbitrageableQuantity();
        BigDecimal netProfit   = computeNetProfit(slippage, quantity, opportunity);
        boolean wasProfitable  = netProfit.compareTo(BigDecimal.ZERO) > 0;

        SimulationResult result = SimulationResult.builder()
                .id(UUID.randomUUID())
                .opportunityId(opportunity.getId())
                .simulatedLatencyMs(executionLatencyMs)
                .slippageBps(slippage.getTotalSlippageBps())
                .simulatedBuyPrice(slippage.getBuyExecutionPrice())
                .simulatedSellPrice(slippage.getSellExecutionPrice())
                .simulatedQuantity(quantity)
                .wasProfitable(wasProfitable)
                .netProfit(netProfit)
                .fillProbability(FULL_FILL_PROBABILITY)
                .simulationTimestamp(Instant.now())
                .build();

        simulationResultRepository.save(result);

        log.info("Simulation persisted: opportunityId={} latencyMs={} slippageBps={} " +
                 "netProfit={} profitable={} priceDataAvailable={}",
                opportunity.getId(), executionLatencyMs, slippage.getTotalSlippageBps(),
                netProfit, wasProfitable, slippage.isPriceDataAvailable());
    }

    /**
     * Computes net profit using execution prices (not detection prices).
     *
     * <p>Formula: {@code (sellPrice − buyPrice) × qty − buyFee × qty − sellFee × qty},
     * where prices are from the slippage estimator (prices at fill time).
     */
    private BigDecimal computeNetProfit(SlippageResult slippage,
                                        BigDecimal quantity,
                                        ArbitrageOpportunity opportunity) {
        BigDecimal buyPrice  = slippage.getBuyExecutionPrice();
        BigDecimal sellPrice = slippage.getSellExecutionPrice();

        BigDecimal grossProfit = sellPrice.subtract(buyPrice)
                .multiply(quantity)
                .setScale(PROFIT_SCALE, RoundingMode.HALF_UP);

        BigDecimal buyFee = buyPrice.multiply(opportunity.getBuyFeeRate())
                .multiply(quantity)
                .setScale(PROFIT_SCALE, RoundingMode.HALF_UP);

        BigDecimal sellFee = sellPrice.multiply(opportunity.getSellFeeRate())
                .multiply(quantity)
                .setScale(PROFIT_SCALE, RoundingMode.HALF_UP);

        return grossProfit.subtract(buyFee).subtract(sellFee);
    }
}
