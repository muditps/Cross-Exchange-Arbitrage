package com.arbitrage.connector;

import com.arbitrage.common.model.TradingPair;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

/**
 * Spring {@link ConfigurationProperties} for the list of trading pairs to monitor.
 *
 * <p>Binds the {@code arbitrage.trading.pairs} list from {@code application.yml}.
 * The registry uses this list to determine which pairs to connect to across all
 * enabled exchanges on startup.</p>
 *
 * <p><b>Example YAML:</b></p>
 * <pre>{@code
 * arbitrage:
 *   trading:
 *     pairs:
 *       - BTC-USDT
 *       - ETH-USDT
 * }</pre>
 *
 * <p><b>Format:</b> Each entry must be a canonical {@code BASE-QUOTE} string
 * (e.g., {@code BTC-USDT}). Parsing is delegated to
 * {@link TradingPair#fromSymbol(String)}, which enforces the format and
 * throws {@link IllegalArgumentException} on startup for invalid entries.</p>
 *
 *
 * @see com.arbitrage.connector.registry.ExchangeConnectorRegistry for how pairs are used
 * @see TradingPair#fromSymbol(String) for the parsing rules
 */
@ConfigurationProperties(prefix = "arbitrage.trading")
@Validated
@Getter
@Setter
public class TradingPairsProperties {

    /**
     * Ordered list of canonical trading pair symbols to monitor.
     * Each entry must be in {@code BASE-QUOTE} format (e.g., {@code BTC-USDT}).
     * At least one pair is required — the system has nothing to detect without a pair.
     */
    @NotEmpty(message = "At least one trading pair must be configured under arbitrage.trading.pairs")
    private List<String> pairs;

    /**
     * Converts the configured symbol strings to {@link TradingPair} value objects.
     *
     * <p>Parsing happens at call time (not at bind time), so any {@link IllegalArgumentException}
     * for invalid symbol formats will surface when the registry reads pairs on startup rather
     * than silently at properties binding.</p>
     *
     * @return immutable list of parsed trading pairs in configuration order
     * @throws IllegalArgumentException if any symbol is not in {@code BASE-QUOTE} format
     */
    public List<TradingPair> asTradingPairs() {
        return pairs.stream()
                .map(TradingPair::fromSymbol)
                .toList();
    }
}
