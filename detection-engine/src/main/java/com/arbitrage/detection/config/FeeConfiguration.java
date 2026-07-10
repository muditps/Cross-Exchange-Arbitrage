package com.arbitrage.detection.config;

import com.arbitrage.common.model.ExchangeId;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/**
 * Externalized fee configuration for all supported exchanges.
 *
 * <p>Fee rates are bound from {@code application.yml} under the {@code exchanges} prefix.
 * Each exchange has a distinct {@link ExchangeFeeConfig} with a taker rate (used for
 * market orders — our default) and a maker rate (limit orders — reserved for future use).
 *
 * <p>Registering via {@code @EnableConfigurationProperties(FeeConfiguration.class)} in
 * {@link com.arbitrage.detection.DetectionEngineApplication} makes this a Spring bean.
 * Override values without recompiling — use per-exchange environment variables:
 * {@code EXCHANGE_BINANCE_TAKER_FEE}, {@code EXCHANGE_BYBIT_TAKER_FEE},
 * {@code EXCHANGE_KUCOIN_TAKER_FEE}.
 *
 * <p><b>Why YAML instead of the {@code ExchangeId} enum?</b>
 * Hardcoding fee rates in the enum couples business-configuration to domain model code.
 * Rates differ between account tiers (standard vs VIP), change with exchange policy updates,
 * and may differ per trading pair. YAML-backed properties let an operator tune fees for
 * their account tier without touching compiled code.
 *
 * <p><b>Validation:</b> All fee rates are validated at startup via Bean Validation
 * ({@code @Validated}). A misconfigured fee rate (e.g., zero or negative) is caught
 * immediately at boot rather than silently producing incorrect spread calculations.
 */
@ConfigurationProperties(prefix = "exchanges")
@Validated
@Getter
@Setter
public class FeeConfiguration {

    @Valid
    @NotNull
    private ExchangeFeeConfig binance = new ExchangeFeeConfig();

    @Valid
    @NotNull
    private ExchangeFeeConfig bybit = new ExchangeFeeConfig();

    @Valid
    @NotNull
    private ExchangeFeeConfig kucoin = new ExchangeFeeConfig();

    /**
     * NSE fee placeholder. Actual multi-component Indian equity fee model
     * (STT + exchange charges + GST + SEBI levy + stamp duty + brokerage ~0.20% total)
     * is implemented in IndianEquityFeeCalculator (Phase 7C.1).
     */
    @Valid
    @NotNull
    private ExchangeFeeConfig nse = new ExchangeFeeConfig(new java.math.BigDecimal("0.0020"),
            new java.math.BigDecimal("0.0020"));

    /** BSE fee placeholder — same structure as NSE. Full model in Phase 7C.1. */
    @Valid
    @NotNull
    private ExchangeFeeConfig bse = new ExchangeFeeConfig(new java.math.BigDecimal("0.0020"),
            new java.math.BigDecimal("0.0020"));

    /**
     * Returns the configured taker fee rate for the given exchange.
     *
     * <p>Taker fees apply when placing market orders (crossing the spread). Our arbitrage
     * model always assumes taker orders because execution speed is paramount — placing a
     * limit order to save a few bps risks missing the window entirely.
     *
     * @param exchangeId the exchange to look up
     * @return taker fee rate as a decimal fraction (e.g., {@code 0.0010} for 0.10%)
     * @throws IllegalArgumentException if {@code exchangeId} is null or unmapped
     */
    public BigDecimal getTakerFeeRate(ExchangeId exchangeId) {
        return switch (exchangeId) {
            case BINANCE -> binance.getTakerFeeRate();
            case BYBIT   -> bybit.getTakerFeeRate();
            case KUCOIN  -> kucoin.getTakerFeeRate();
            case NSE     -> nse.getTakerFeeRate();
            case BSE     -> bse.getTakerFeeRate();
        };
    }

    /**
     * Returns the configured maker fee rate for the given exchange.
     *
     * <p>Maker fees apply when placing limit orders that add liquidity. Currently unused
     * in spread calculations (Phase 3 assumes taker orders only), but available for
     * the execution simulator to model limit-order strategies in Phase 4.
     *
     * @param exchangeId the exchange to look up
     * @return maker fee rate as a decimal fraction (e.g., {@code 0.0002} for 0.02%)
     * @throws IllegalArgumentException if {@code exchangeId} is null or unmapped
     */
    public BigDecimal getMakerFeeRate(ExchangeId exchangeId) {
        return switch (exchangeId) {
            case BINANCE -> binance.getMakerFeeRate();
            case BYBIT   -> bybit.getMakerFeeRate();
            case KUCOIN  -> kucoin.getMakerFeeRate();
            case NSE     -> nse.getMakerFeeRate();
            case BSE     -> bse.getMakerFeeRate();
        };
    }

    /**
     * Per-exchange fee rates.
     *
     * <p>Defaults are set to standard API tier rates (0.10% taker, 0.02% maker).
     * Accounts with higher volume or BNB/native token discounts should override these
     * via environment variables to produce accurate net spread calculations.
     *
     * <p>Both rates are validated strictly positive at startup — a zero fee would be
     * incorrect for all current exchanges and would inflate net spread calculations.
     */
    @Getter
    @Setter
    public static class ExchangeFeeConfig {

        /**
         * Taker fee rate (market order). Default: 0.10% (0.0010).
         * Override via e.g. {@code EXCHANGE_BINANCE_TAKER_FEE=0.00075} for VIP tier.
         */
        @NotNull
        @DecimalMin(value = "0.0", inclusive = false)
        private BigDecimal takerFeeRate = new BigDecimal("0.0010");

        /**
         * Maker fee rate (limit order). Default: 0.02% (0.0002).
         * Override via e.g. {@code EXCHANGE_BINANCE_MAKER_FEE=0.0001} for VIP tier.
         */
        @NotNull
        @DecimalMin(value = "0.0", inclusive = false)
        private BigDecimal makerFeeRate = new BigDecimal("0.0002");

        public ExchangeFeeConfig() {}

        public ExchangeFeeConfig(BigDecimal takerFeeRate, BigDecimal makerFeeRate) {
            this.takerFeeRate = takerFeeRate;
            this.makerFeeRate = makerFeeRate;
        }
    }
}
