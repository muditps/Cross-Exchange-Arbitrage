package com.arbitrage.connector;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import reactor.core.publisher.Flux;

/**
 * Strategy Pattern interface for exchange market data connectors.
 *
 * <p>Every exchange (Binance, Bybit, KuCoin, and future NSE/BSE) implements this
 * interface. Downstream modules (normalisation engine, detection engine, dashboard)
 * consume {@link NormalisedTick} objects without knowing which exchange produced them.
 * Adding a new exchange means implementing this interface — zero changes to the
 * rest of the pipeline.</p>
 *
 * <p><b>Lifecycle:</b> A connector starts in {@link FeedStatus#DISCONNECTED} state.
 * Calling {@link #connect(TradingPair)} opens a WebSocket connection and transitions
 * to {@link FeedStatus#CONNECTED}. If the connection drops, the connector transitions
 * to {@link FeedStatus#RECONNECTING} and attempts exponential backoff recovery.
 * If no messages arrive within the staleness threshold while connected, the status
 * transitions to {@link FeedStatus#STALE}. Calling {@link #disconnect()} terminates
 * the connection and all active streams.</p>
 *
 * <p><b>Multi-pair usage:</b> Call {@code connect()} once per trading pair. To monitor
 * multiple pairs, call {@code connect()} for each and merge the resulting streams:
 * {@code Flux.merge(connector.connect(btcUsdt), connector.connect(ethUsdt))}.</p>
 *
 * <p><b>Threading model:</b> The returned {@link Flux} emits on Reactor's event loop
 * threads. Subscribers must not block — use {@code subscribeOn()} or {@code publishOn()}
 * to offload heavy processing.</p>
 *
 * @see ExchangeConnectionException for connection failure error type
 * @see FeedStatus for the state machine governing feed health
 */
public interface ExchangeConnector {

    /**
     * Opens a WebSocket connection to the exchange and returns a reactive stream
     * of normalised ticks for the given trading pair.
     *
     * <p>The returned {@link Flux} is <b>hot</b> — it emits ticks as they arrive
     * from the exchange in real-time. The connector manages the WebSocket lifecycle
     * internally (connection, heartbeat, reconnection). Subscribers receive ticks
     * until they cancel the subscription or {@link #disconnect()} is called.</p>
     *
     * <p><b>Latency-critical:</b> The connector stamps
     * {@link NormalisedTick#getReceivedTimestamp()} with {@code System.nanoTime()}
     * as the very first operation upon receiving a WebSocket message — before parsing,
     * before logging, before anything. This timestamp is the ground truth (T0) for
     * measuring end-to-end pipeline latency.</p>
     *
     * <p>Connection failures and parse errors are signaled through the Flux's error
     * channel as {@link ExchangeConnectionException}.</p>
     *
     * @param tradingPair the pair to subscribe to (e.g., BTC-USDT)
     * @return a hot Flux emitting {@link NormalisedTick} objects as they arrive
     */
    Flux<NormalisedTick> connect(TradingPair tradingPair);

    /**
     * Gracefully disconnects from the exchange, closing the WebSocket connection
     * and completing all active Flux streams.
     *
     * <p>After {@code disconnect()}, {@link #getFeedStatus()} returns
     * {@link FeedStatus#DISCONNECTED}. A new {@link #connect(TradingPair)} call
     * is required to resume receiving ticks.</p>
     *
     * <p>This method is idempotent — calling it on an already-disconnected
     * connector has no effect.</p>
     */
    void disconnect();

    /**
     * Returns the identifier of the exchange this connector is bound to.
     *
     * <p>Each connector instance is associated with exactly one exchange.
     * This identifier is used for logging, metrics tagging, Redis key construction,
     * and Kafka topic routing.</p>
     *
     * @return the exchange identifier (e.g., {@link ExchangeId#BINANCE})
     */
    ExchangeId getExchangeId();

    /**
     * Returns the current health status of the WebSocket feed.
     *
     * <p>Downstream consumers use this to decide whether to trust incoming data.
     * The detection engine skips comparisons involving data from a connector whose
     * status is not {@link FeedStatus#CONNECTED}.</p>
     *
     * <p><b>State transitions:</b></p>
     * <ul>
     *   <li>{@code DISCONNECTED → CONNECTED} — WebSocket handshake succeeds</li>
     *   <li>{@code CONNECTED → STALE} — no message received within staleness threshold</li>
     *   <li>{@code CONNECTED → RECONNECTING} — connection dropped unexpectedly</li>
     *   <li>{@code STALE → CONNECTED} — message received after stale period</li>
     *   <li>{@code RECONNECTING → CONNECTED} — reconnection succeeded</li>
     *   <li>{@code RECONNECTING → DISCONNECTED} — all retry attempts exhausted</li>
     * </ul>
     *
     * @return the current feed status
     */
    FeedStatus getFeedStatus();
}
