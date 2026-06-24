package com.arbitrage.connector.reconnect;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.connector.ExchangeConnectionException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;

/**
 * Reusable exponential backoff reconnection strategy for exchange connectors.
 *
 * <p><b>What it does:</b> Builds a Reactor {@link Retry} spec with exponential
 * backoff and jitter. Any exchange connector calls {@link #buildRetrySpec()} to
 * get a retry operator that handles automatic reconnection with proper delay
 * progression.</p>
 *
 * <p><b>Delay progression (default):</b> 1s → 2s → 4s → 8s → 16s → 30s (max).
 * Each attempt doubles the previous delay until reaching {@code maxDelay}. This
 * is exponential backoff — the standard pattern for handling transient failures
 * without overwhelming the remote service.</p>
 *
 * <p><b>Why jitter?</b> Without jitter, if 1000 clients disconnect at the same
 * moment, they all retry at 1s, then 2s, then 4s — in perfect lockstep. This
 * "thundering herd" hammers the exchange and can cause it to fail again. Jitter
 * adds random variation to each delay (Reactor's {@link Retry#backoff} includes
 * 50% jitter by default — a delay of 4s becomes randomly 2s-6s), spreading
 * reconnection attempts over time. AWS SDK, Google Cloud SDK, and every
 * production client library uses this pattern.</p>
 *
 * <p><b>Why a separate class?</b> In Session 1.2, the retry logic was inline in
 * BinanceConnector — tangled with Binance-specific callbacks. When Bybit and
 * KuCoin connectors are added, they need the same backoff math with their own
 * callbacks. Extracting the strategy means: one place for backoff configuration,
 * independently testable delay progression, and consistent reconnection behaviour
 * across all exchanges.</p>
 *
 * <p><b>Thread safety:</b> This class is immutable after construction — all fields
 * are final. The built {@link RetryBackoffSpec} is also immutable. Safe to share
 * across threads and connectors.</p>
 *
 * @see com.arbitrage.connector.ExchangeConnectorProperties.ExchangeProperties for
 *      the config source
 */
@Slf4j
@Getter
public class ExponentialBackoffReconnectStrategy {

    /** Which exchange this strategy serves — used for structured logging and exceptions. */
    private final ExchangeId exchangeId;

    /** Delay before the first retry attempt. Doubles each subsequent attempt. */
    private final Duration initialDelay;

    /** Maximum delay between retry attempts. Caps the exponential growth. */
    private final Duration maxDelay;

    /** Maximum number of retry attempts before giving up permanently. */
    private final long maxAttempts;

    /**
     * Creates a reconnect strategy with the given parameters.
     *
     * @param exchangeId   the exchange this strategy serves (for logging/exceptions)
     * @param initialDelay delay before the first retry (e.g., 1s)
     * @param maxDelay     maximum delay cap (e.g., 30s)
     * @param maxAttempts  maximum retry attempts before permanent failure (e.g., 10)
     */
    public ExponentialBackoffReconnectStrategy(ExchangeId exchangeId,
                                               Duration initialDelay,
                                               Duration maxDelay,
                                               long maxAttempts) {
        this.exchangeId = exchangeId;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Builds a Reactor {@link RetryBackoffSpec} configured with exponential backoff
     * and jitter.
     *
     * <p>The returned spec can be passed to {@code Flux.retryWhen()} or
     * {@code Mono.retryWhen()} on any reactive pipeline. It handles:</p>
     * <ul>
     *   <li>Exponential delay progression: {@code initialDelay * 2^attempt}</li>
     *   <li>Delay cap at {@code maxDelay}</li>
     *   <li>Built-in jitter (Reactor adds 50% jitter by default — a 4s delay
     *       becomes randomly 2s-6s)</li>
     *   <li>Structured logging before each retry (attempt number, error message)</li>
     *   <li>Typed exception on exhaustion ({@link ExchangeConnectionException}
     *       with the exchange ID)</li>
     * </ul>
     *
     * <p><b>Connector-specific callbacks</b> (e.g., setting feed status to
     * RECONNECTING) are NOT included here — they belong in the connector.
     * The connector chains its own {@code doBeforeRetry()} after calling
     * this method. This separation keeps the strategy reusable across exchanges.</p>
     *
     * @return a configured RetryBackoffSpec ready for use with retryWhen()
     */
    public RetryBackoffSpec buildRetrySpec() {
        return Retry.backoff(maxAttempts, initialDelay)
                .maxBackoff(maxDelay)
                .doBeforeRetry(signal ->
                        log.warn("{} reconnecting: attempt={}/{} error={}",
                                exchangeId,
                                signal.totalRetries() + 1,
                                maxAttempts,
                                signal.failure().getMessage()))
                .onRetryExhaustedThrow((spec, signal) ->
                        new ExchangeConnectionException(exchangeId,
                                String.format("All %d reconnection attempts exhausted for %s. "
                                                + "Last error: %s",
                                        signal.totalRetries(),
                                        exchangeId,
                                        signal.failure().getMessage()),
                                signal.failure()));
    }

    /**
     * Returns a human-readable description of this strategy's configuration.
     * Useful for startup logging ("Binance reconnect: 1s→30s max, 10 attempts").
     *
     * @return a formatted string describing the backoff parameters
     */
    @Override
    public String toString() {
        return String.format("%s reconnect strategy: initialDelay=%s, maxDelay=%s, maxAttempts=%d",
                exchangeId, initialDelay, maxDelay, maxAttempts);
    }
}
