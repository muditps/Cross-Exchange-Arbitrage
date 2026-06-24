package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Normalisation transformer for KuCoin ticks.
 *
 * <p><b>Exchange-specific notes:</b>
 * <ul>
 *   <li><b>Symbol format:</b> KuCoin uses {@code BTC-USDT} — identical to our canonical
 *       format. No symbol conversion is required. This is unique among the three supported
 *       exchanges (Binance and Bybit both need conversion).</li>
 *   <li><b>Exchange timestamp:</b> KuCoin provides a millisecond server-side timestamp
 *       in {@code data.time}. The connector uses {@code Instant.ofEpochMilli(data.getTime())},
 *       enabling accurate clock skew measurement — same as Bybit. This transformer preserves
 *       {@code exchangeTimestamp} unchanged.</li>
 *   <li><b>Connection protocol:</b> KuCoin requires a REST POST to
 *       {@code bullet-public} before WebSocket connection. This is handled entirely by
 *       {@code KuCoinTokenService} and {@code KuCoinConnector} in the exchange-connectors
 *       module. This transformer only sees the resulting {@link NormalisedTick}.</li>
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
public class KuCoinTickTransformer implements TickTransformer {

    @Override
    public ExchangeId supports() {
        return ExchangeId.KUCOIN;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@link Optional#empty()} if the inbound tick is null, if any required
     * price or quantity field is null, or if the tick's exchangeId is not
     * {@link ExchangeId#KUCOIN}.</p>
     */
    @Override
    public Optional<NormalisedTick> transform(NormalisedTick inboundTick, long normalisationStartNanos) {
        if (inboundTick == null) {
            log.warn("Received null inbound tick in KuCoinTickTransformer");
            return Optional.empty();
        }

        if (inboundTick.getExchangeId() != ExchangeId.KUCOIN) {
            log.warn("KuCoinTickTransformer received tick from wrong exchange: exchangeId={}",
                    inboundTick.getExchangeId());
            return Optional.empty();
        }

        if (!hasValidPriceFields(inboundTick)) {
            log.warn("KuCoin tick has null price/quantity fields: pair={}",
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

        log.debug("KuCoin tick normalised: pair={} bid={} ask={} latencyNs={}",
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
