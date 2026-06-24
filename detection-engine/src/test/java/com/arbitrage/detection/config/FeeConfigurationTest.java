package com.arbitrage.detection.config;

import com.arbitrage.common.model.ExchangeId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FeeConfiguration}.
 *
 * <p>Verifies default fee rates, per-exchange routing, and override behaviour.
 * These tests construct {@link FeeConfiguration} directly — no Spring context needed
 * because {@code @ConfigurationProperties} binding is tested at integration level.
 */
class FeeConfigurationTest {

    private FeeConfiguration feeConfiguration;

    @BeforeEach
    void setUp() {
        feeConfiguration = new FeeConfiguration();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Default taker fee rates
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("default taker fee rates are 0.10% for all exchanges")
    void defaultTakerFeeRates_areStandardTier() {
        BigDecimal expected = new BigDecimal("0.0010");

        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BINANCE))
                .isEqualByComparingTo(expected);
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(expected);
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.KUCOIN))
                .isEqualByComparingTo(expected);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Default maker fee rates
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("default maker fee rates are 0.02% for all exchanges")
    void defaultMakerFeeRates_areStandardTier() {
        BigDecimal expected = new BigDecimal("0.0002");

        assertThat(feeConfiguration.getMakerFeeRate(ExchangeId.BINANCE))
                .isEqualByComparingTo(expected);
        assertThat(feeConfiguration.getMakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(expected);
        assertThat(feeConfiguration.getMakerFeeRate(ExchangeId.KUCOIN))
                .isEqualByComparingTo(expected);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Per-exchange override routing
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("overriding Binance taker fee is returned for BINANCE only")
    void overrideBinanceTakerFee_onlyBinanceAffected() {
        BigDecimal vipRate = new BigDecimal("0.00075");
        feeConfiguration.getBinance().setTakerFeeRate(vipRate);

        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BINANCE))
                .isEqualByComparingTo(vipRate);
        // Other exchanges must be unchanged
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(new BigDecimal("0.0010"));
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.KUCOIN))
                .isEqualByComparingTo(new BigDecimal("0.0010"));
    }

    @Test
    @DisplayName("overriding KuCoin taker fee isolates change to KUCOIN routing")
    void overrideKucoinTakerFee_onlyKucoinAffected() {
        BigDecimal discountRate = new BigDecimal("0.0008");
        feeConfiguration.getKucoin().setTakerFeeRate(discountRate);

        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.KUCOIN))
                .isEqualByComparingTo(discountRate);
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BINANCE))
                .isEqualByComparingTo(new BigDecimal("0.0010"));
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(new BigDecimal("0.0010"));
    }

    @Test
    @DisplayName("taker and maker fee rates are independent per exchange")
    void takerAndMakerRates_areIndependent() {
        BigDecimal customTaker = new BigDecimal("0.0007");
        BigDecimal customMaker = new BigDecimal("0.0001");
        feeConfiguration.getBybit().setTakerFeeRate(customTaker);
        feeConfiguration.getBybit().setMakerFeeRate(customMaker);

        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(customTaker);
        assertThat(feeConfiguration.getMakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(customMaker);
    }

    @Test
    @DisplayName("all three exchanges can have different taker fee rates simultaneously")
    void differentTakerFeePerExchange_allReturnedCorrectly() {
        feeConfiguration.getBinance().setTakerFeeRate(new BigDecimal("0.0010"));
        feeConfiguration.getBybit().setTakerFeeRate(new BigDecimal("0.0006"));
        feeConfiguration.getKucoin().setTakerFeeRate(new BigDecimal("0.0008"));

        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BINANCE))
                .isEqualByComparingTo(new BigDecimal("0.0010"));
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.BYBIT))
                .isEqualByComparingTo(new BigDecimal("0.0006"));
        assertThat(feeConfiguration.getTakerFeeRate(ExchangeId.KUCOIN))
                .isEqualByComparingTo(new BigDecimal("0.0008"));
    }
}
