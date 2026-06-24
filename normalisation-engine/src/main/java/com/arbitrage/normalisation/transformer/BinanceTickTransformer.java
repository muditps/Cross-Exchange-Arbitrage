package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Normalisation transformer for Binance ticks.
 *
 * <p><b>Exchange-specific notes:</b>
 * <ul>
 *   <li><b>Symbol format:</b> Binance sends {@code BTCUSDT} (lowercase, no separator).
 *       This is converted to canonical {@code BTC-USDT} by {@code BinanceMessageParser}
 *       in the exchange-connectors module before the tick reaches Kafka. This transformer
 *       receives an already-canonicalised {@link NormalisedTick}.</li>
 *   <li><b>Exchange timestamp:</b> Binance's {@code bookTicker} stream does NOT include
 *       a server-side timestamp. The connector uses {@code Instant.now()} as a fallback.
 *       This means clock skew measurement is not available for Binance bookTicker data.</li>
 *   <li><b>Price precision:</b> Binance prices arrive as 8-decimal-place strings
 *       (e.g., {@code "67250.50000000"}). These are parsed to {@link java.math.BigDecimal}
 *       by the connector and must pass through this transformer without precision loss.</li>
 * </ul>
 *
 * <p><b>Current behaviour:</b> Re-stamps {@code processedTimestamp} to T4
 * ({@code System.nanoTime()} at the moment this transformer runs). All other fields
 * from the inbound tick are preserved unchanged.
 *
 * <p><b>Thread safety:</b> This class is stateless. It is safe to call
 * {@link #transform} concurrently from multiple Kafka listener threads.
 */
@Component
@Slf4j
public class BinanceTickTransformer implements TickTransformer {

    @Override
    public ExchangeId supports() {
        return ExchangeId.BINANCE;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@link Optional#empty()} if the inbound tick is null, if any required
     * price or quantity field is null, or if the tick's exchangeId is not
     * {@link ExchangeId#BINANCE}.</p>
     */
    @Override
    public Optional<NormalisedTick> transform(NormalisedTick inboundTick, long normalisationStartNanos) {
        if (inboundTick == null) {
            log.warn("Received null inbound tick in BinanceTickTransformer");
            return Optional.empty();
        }

        if (inboundTick.getExchangeId() != ExchangeId.BINANCE) {
            log.warn("BinanceTickTransformer received tick from wrong exchange: exchangeId={}",
                    inboundTick.getExchangeId());
            return Optional.empty();
        }

        if (!hasValidPriceFields(inboundTick)) {
            log.warn("Binance tick has null price/quantity fields: pair={}",
                    inboundTick.getTradingPair());
            return Optional.empty();
        }

        // T4 — normalisation engine processing complete
        final long processedNanos = System.nanoTime();

        NormalisedTick normalisedTick = NormalisedTick.builder()
                .exchangeId(inboundTick.getExchangeId())
                .tradingPair(inboundTick.getTradingPair())
                .bestBidPrice(inboundTick.getBestBidPrice())
                .bestAskPrice(inboundTick.getBestAskPrice())
                .bestBidQuantity(inboundTick.getBestBidQuantity())
                .bestAskQuantity(inboundTick.getBestAskQuantity())
                .exchangeTimestamp(inboundTick.getExchangeTimestamp())
                .receivedTimestamp(inboundTick.getReceivedTimestamp())   // T0 preserved — staleness anchor
                .processedTimestamp(processedNanos)                       // T4 — updated by normalisation engine
                .build();

        log.debug("Binance tick normalised: pair={} bid={} ask={} latencyNs={}",
                normalisedTick.getTradingPair().canonicalSymbol(),
                normalisedTick.getBestBidPrice(),
                normalisedTick.getBestAskPrice(),
                processedNanos - normalisationStartNanos);

        return Optional.of(normalisedTick);
    }

    /**
     * Validates that all price and quantity fields required for arbitrage detection are present.
     *
     * @param tick the tick to validate
     * @return true if all required fields are non-null
     */
    private boolean hasValidPriceFields(NormalisedTick tick) {
        return tick.getBestBidPrice() != null
                && tick.getBestAskPrice() != null
                && tick.getBestBidQuantity() != null
                && tick.getBestAskQuantity() != null
                && tick.getTradingPair() != null
                && tick.getExchangeTimestamp() != null;
    }
}
