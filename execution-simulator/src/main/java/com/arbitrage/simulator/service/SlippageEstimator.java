package com.arbitrage.simulator.service;

import com.arbitrage.common.model.ArbitrageOpportunity;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.simulator.config.SimulationProperties;
import com.arbitrage.simulator.model.SlippageResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Optional;

/**
 * Estimates price slippage for a two-legged arbitrage trade by comparing
 * detection-time prices against prices observed at the simulated fill time.
 *
 * <p><b>Why slippage matters:</b> The detection engine records prices at T₀ (detection time).
 * The execution simulator computes that fills happened at T₁ = T₀ + executionLatencyMs.
 * Between T₀ and T₁, prices move. This estimator quantifies that movement in basis points
 * per leg so downstream components can compute realistic simulated P&L.
 *
 * <p><b>Formula:</b>
 * <ul>
 *   <li>Buy slippage (bps) = {@code (executionAsk − detectionAsk) / detectionAsk × 10,000}</li>
 *   <li>Sell slippage (bps) = {@code (detectionBid − executionBid) / detectionBid × 10,000}</li>
 * </ul>
 * Positive = adverse. Negative = favorable. Total = sum of both legs.
 *
 * <p><b>Fallback behaviour:</b> When {@link PriceAtExecutionLookup} returns empty for
 * either leg (price store not yet warm on startup), this estimator falls back to:
 * <ul>
 *   <li>Execution prices = detection-time prices (no market movement assumed)</li>
 *   <li>Slippage = {@code defaultSlippageBps} per leg from {@link SimulationProperties}</li>
 *   <li>{@code priceDataAvailable = false} on the returned result</li>
 * </ul>
 * The fallback represents a conservative worst-case assumption rather than observed data.
 * {@code defaultSlippageBps = 2.00} (configured in {@code application.yml}) means we assume
 * 2 bps of adverse slippage per leg = 4 bps total when price history is unavailable.
 *
 * <p><b>This is the first consumer of {@code SimulationProperties.defaultSlippageBps}.</b>
 * That field was configured in Session 4.1 specifically for this fallback path.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SlippageEstimator {

    private static final int BPS_SCALE = 4;
    private static final int PRICE_SCALE = 8;
    private static final BigDecimal BPS_MULTIPLIER = BigDecimal.valueOf(10_000);

    private final PriceAtExecutionLookup priceAtExecutionLookup;
    private final SimulationProperties simulationProperties;

    /**
     * Estimates the slippage for a closed arbitrage opportunity at the simulated fill time.
     *
     * <p>The fill time is computed as {@code opportunity.detectionTimestamp + executionLatencyMs}.
     * Prices at that instant are retrieved via floor lookup from the
     * {@link HistoricalPriceStore}. If data is unavailable for either leg, the
     * configured {@code defaultSlippageBps} is applied and the result is flagged accordingly.
     *
     * @param opportunity        the closed opportunity containing detection-time prices and metadata
     * @param executionLatencyMs total simulated execution time in ms (from ExecutionTimelineSimulator)
     * @return slippage result with per-leg and total bps, and a data-availability flag
     */
    public SlippageResult estimate(ArbitrageOpportunity opportunity, long executionLatencyMs) {
        Instant targetTime = opportunity.getDetectionTimestamp().plusMillis(executionLatencyMs);

        Optional<NormalisedTick> buyTick = priceAtExecutionLookup.findClosestBefore(
                opportunity.getBuyExchange(), opportunity.getTradingPair(), targetTime);
        Optional<NormalisedTick> sellTick = priceAtExecutionLookup.findClosestBefore(
                opportunity.getSellExchange(), opportunity.getTradingPair(), targetTime);

        if (buyTick.isEmpty() || sellTick.isEmpty()) {
            log.warn("Price data unavailable for opportunity={} buyAvailable={} sellAvailable={} — " +
                     "using defaultSlippageBps={}",
                    opportunity.getId(),
                    buyTick.isPresent(),
                    sellTick.isPresent(),
                    simulationProperties.getDefaultSlippageBps());
            return buildFallbackResult(opportunity);
        }

        BigDecimal buyExecutionPrice  = buyTick.get().getBestAskPrice();
        BigDecimal sellExecutionPrice = sellTick.get().getBestBidPrice();

        BigDecimal buySlippageBps  = computeBuySlippageBps(opportunity.getBuyPrice(), buyExecutionPrice);
        BigDecimal sellSlippageBps = computeSellSlippageBps(opportunity.getSellPrice(), sellExecutionPrice);

        log.debug("Slippage estimated: id={} buySlippage={}bps sellSlippage={}bps target={}",
                opportunity.getId(), buySlippageBps, sellSlippageBps, targetTime);

        return SlippageResult.builder()
                .buyExecutionPrice(buyExecutionPrice)
                .sellExecutionPrice(sellExecutionPrice)
                .buySlippageBps(buySlippageBps)
                .sellSlippageBps(sellSlippageBps)
                .totalSlippageBps(buySlippageBps.add(sellSlippageBps))
                .priceDataAvailable(true)
                .build();
    }

    /**
     * Computes buy-leg slippage in basis points.
     *
     * <p>Positive when we paid more at execution than at detection — the ask price rose
     * during the execution window. Formula:
     * {@code (executionAsk − detectionAsk) / detectionAsk × 10,000}.
     */
    private BigDecimal computeBuySlippageBps(BigDecimal detectionAsk, BigDecimal executionAsk) {
        return executionAsk.subtract(detectionAsk)
                .divide(detectionAsk, PRICE_SCALE, RoundingMode.HALF_UP)
                .multiply(BPS_MULTIPLIER)
                .setScale(BPS_SCALE, RoundingMode.HALF_UP);
    }

    /**
     * Computes sell-leg slippage in basis points.
     *
     * <p>Positive when we received less at execution than at detection — the bid price fell
     * during the execution window. Formula:
     * {@code (detectionBid − executionBid) / detectionBid × 10,000}.
     */
    private BigDecimal computeSellSlippageBps(BigDecimal detectionBid, BigDecimal executionBid) {
        return detectionBid.subtract(executionBid)
                .divide(detectionBid, PRICE_SCALE, RoundingMode.HALF_UP)
                .multiply(BPS_MULTIPLIER)
                .setScale(BPS_SCALE, RoundingMode.HALF_UP);
    }

    /**
     * Builds a fallback result using detection-time prices and the configured default slippage.
     *
     * <p>Called when the price store does not have data for one or both legs.
     * The result uses detection-time prices as execution prices (no market movement assumed)
     * and applies {@code defaultSlippageBps} per leg as a conservative estimate.
     */
    private SlippageResult buildFallbackResult(ArbitrageOpportunity opportunity) {
        BigDecimal perLegSlippage = simulationProperties.getDefaultSlippageBps();
        return SlippageResult.builder()
                .buyExecutionPrice(opportunity.getBuyPrice())
                .sellExecutionPrice(opportunity.getSellPrice())
                .buySlippageBps(perLegSlippage)
                .sellSlippageBps(perLegSlippage)
                .totalSlippageBps(perLegSlippage.add(perLegSlippage))
                .priceDataAvailable(false)
                .build();
    }
}
