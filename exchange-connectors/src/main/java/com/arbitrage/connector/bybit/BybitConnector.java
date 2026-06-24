package com.arbitrage.connector.bybit;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnectionException;
import com.arbitrage.connector.ExchangeConnector;
import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.TradingPairsProperties;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.arbitrage.connector.reconnect.ExponentialBackoffReconnectStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Bybit exchange connector that subscribes to the V5 Spot Tickers WebSocket stream
 * and emits {@link NormalisedTick} objects for each price update.
 *
 * <p><b>Bybit Tickers stream</b> provides real-time top-of-book bid/ask price and
 * quantity updates. Both "snapshot" (full state) and "delta" (incremental) message
 * types contain the same {@code bid1Price}/{@code ask1Price} fields and are treated
 * identically for arbitrage detection.</p>
 *
 * <p><b>Client-initiated heartbeat:</b> Unlike Binance (where the server sends pings),
 * Bybit requires the <em>client</em> to send {@code {"op":"ping"}} every 20 seconds.
 * Missing heartbeats cause Bybit to disconnect the WebSocket. This connector uses
 * {@link Flux#interval(Duration)} to generate periodic ping messages within the
 * WebSocket session pipeline — scoped to the session lifetime so pings automatically
 * stop on disconnect and restart on reconnection.</p>
 *
 * <p><b>Latency discipline:</b> The very first operation in the message handler is
 * {@code System.nanoTime()} — this is T0, the ground truth timestamp for measuring
 * end-to-end pipeline latency. Everything else (parsing, logging, emission) happens
 * after T0 is captured.</p>
 *
 * <p><b>Feed health:</b> A {@link Flux#interval(Duration)} task monitors message
 * freshness. If no message arrives within the configured staleness threshold, the
 * feed status transitions from {@link FeedStatus#CONNECTED} to {@link FeedStatus#STALE}.
 * The detection engine must skip stale prices to avoid false arbitrage signals.</p>
 *
 * @see ExchangeConnector for the Strategy Pattern interface
 * @see NormalisedTick for the output format
 * @see BybitMessageParser for JSON parsing logic
 */
@Component
@Slf4j
public class BybitConnector implements ExchangeConnector {

    /**
     * Heartbeat ping message sent every 20 seconds to keep the connection alive.
     * Bybit disconnects if no ping is received within ~30 seconds.
     */
    private static final String PING_MESSAGE =
            "{\"req_id\":\"heartbeat\",\"op\":\"ping\"}";

    /**
     * Bybit requires client pings every 20 seconds. This is more aggressive than
     * typical WebSocket keepalive intervals because Bybit's timeout is ~30 seconds.
     * The 20-second interval provides a 10-second safety margin.
     */
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(20);

    private final ExchangeConnectorProperties.ExchangeProperties config;
    private final BybitMessageParser messageParser;
    private final TradingPairsProperties tradingPairsProperties;
    private final WebSocketClient webSocketClient;
    private final ExponentialBackoffReconnectStrategy reconnectStrategy;
    private final ConnectorMetrics metrics;

    /**
     * Hot sink that bridges imperative WebSocket callbacks into a reactive Flux.
     *
     * <p>{@code multicast()} means each subscriber gets only messages emitted after
     * they subscribe (no replay). {@code onBackpressureBuffer()} buffers if subscribers
     * are slow — acceptable for Phase 2; bounded buffer with overflow strategy can
     * be added in Phase 6 performance tuning.</p>
     */
    private final Sinks.Many<NormalisedTick> sink;

    /**
     * Current feed health status. Uses {@link AtomicReference} for thread-safe
     * transitions — the WebSocket event loop, staleness checker, heartbeat timer,
     * and consumer threads all read/write this concurrently.
     */
    private final AtomicReference<FeedStatus> feedStatus;

    /**
     * Nanosecond timestamp of the last received WebSocket message.
     * Used by the staleness checker to determine if the feed has gone quiet.
     * A value of 0 means no message has been received yet.
     */
    private final AtomicLong lastMessageNanoTime;

    /** Subscription to the WebSocket connection + retry loop. Cancelled on disconnect. */
    private volatile Disposable connectionDisposable;

    /** Subscription to the staleness checker interval. Cancelled on disconnect. */
    private volatile Disposable stalenessDisposable;

    /**
     * Creates a Bybit connector with configuration from application properties.
     *
     * @param properties             the exchange connector properties containing Bybit config
     * @param messageParser          the parser for converting raw JSON to NormalisedTick
     * @param metrics                connector metrics for recording message rates, errors, and latency
     * @param tradingPairsProperties configured trading pairs used to build multi-pair subscription
     * @throws ExchangeConnectionException if Bybit configuration is missing
     */
    @Autowired
    public BybitConnector(ExchangeConnectorProperties properties,
                          BybitMessageParser messageParser,
                          ConnectorMetrics metrics,
                          TradingPairsProperties tradingPairsProperties) {
        ExchangeConnectorProperties.ExchangeProperties bybitConfig =
                properties.getConnectors().get("bybit");

        if (bybitConfig == null) {
            throw new ExchangeConnectionException(ExchangeId.BYBIT,
                    "Bybit connector configuration is missing from arbitrage.exchanges.connectors.bybit");
        }

        this.config = bybitConfig;
        this.messageParser = messageParser;
        this.tradingPairsProperties = tradingPairsProperties;
        this.webSocketClient = new ReactorNettyWebSocketClient();
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.BYBIT,
                bybitConfig.getInitialReconnectDelay(),
                bybitConfig.getMaxReconnectDelay(),
                bybitConfig.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);

        log.info("Initialised {}", reconnectStrategy);
    }

    /**
     * Package-private constructor for testing — allows injecting a mock WebSocketClient and metrics.
     *
     * @param config          Bybit-specific exchange properties
     * @param messageParser   the parser for converting raw JSON to NormalisedTick
     * @param webSocketClient WebSocket client implementation (real or mock)
     * @param metrics         connector metrics (real or test SimpleMeterRegistry)
     */
    BybitConnector(ExchangeConnectorProperties.ExchangeProperties config,
                   BybitMessageParser messageParser,
                   WebSocketClient webSocketClient,
                   ConnectorMetrics metrics) {
        this.config = config;
        this.messageParser = messageParser;
        this.tradingPairsProperties = null; // tests use single-pair mode
        this.webSocketClient = webSocketClient;
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.BYBIT,
                config.getInitialReconnectDelay(),
                config.getMaxReconnectDelay(),
                config.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Opens a WebSocket connection to Bybit and subscribes to the tickers stream
     * for the given trading pair. The returned Flux emits {@link NormalisedTick}
     * objects in real-time as Bybit pushes price updates.</p>
     *
     * <p>Also starts a client-initiated heartbeat (ping every 20s) and a staleness
     * checker. If already connected, returns the existing Flux without reconnecting.</p>
     *
     * @param tradingPair the pair to subscribe to (e.g., BTC-USDT)
     * @return a hot Flux of normalised ticks
     */
    @Override
    public Flux<NormalisedTick> connect(TradingPair tradingPair) {
        if (feedStatus.get() != FeedStatus.DISCONNECTED) {
            log.warn("BybitConnector already active, status={}. Returning existing Flux.", feedStatus.get());
            return sink.asFlux();
        }

        if (!config.isEnabled()) {
            log.info("Bybit connector is disabled in configuration. Not connecting.");
            return Flux.empty();
        }

        URI wsUri = URI.create(config.getWsEndpoint());
        log.info("Connecting to Bybit WebSocket: endpoint={} pair={}", wsUri, tradingPair.canonicalSymbol());

        startStalenessChecker();

        connectionDisposable = webSocketClient.execute(wsUri,
                        session -> handleSession(session, tradingPair))
                .doOnSubscribe(subscription -> {
                    log.info("Bybit WebSocket subscription started for pair={}", tradingPair.canonicalSymbol());
                })
                .doOnError(error -> {
                    log.warn("Bybit WebSocket error (will retry): pair={} error={}", tradingPair.canonicalSymbol(),
                            error.getMessage());
                })
                .doOnTerminate(() -> {
                    log.warn("Bybit WebSocket terminated for pair={}", tradingPair.canonicalSymbol());
                })
                .retryWhen(reconnectStrategy.buildRetrySpec()
                        .doBeforeRetry(signal -> {
                            feedStatus.set(FeedStatus.RECONNECTING);
                            metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.RECONNECTING);
                            metrics.recordReconnection(ExchangeId.BYBIT);
                        }))
                .doFinally(signalType -> {
                    feedStatus.set(FeedStatus.DISCONNECTED);
                    metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.DISCONNECTED);
                    log.info("Bybit WebSocket connection finalised: signal={}", signalType);
                })
                .subscribe(
                        unused -> { },
                        error -> {
                            log.error("Bybit connection failed permanently: {}", error.getMessage());
                            sink.tryEmitError(new ExchangeConnectionException(
                                    ExchangeId.BYBIT, error.getMessage(), error));
                        }
                );

        return sink.asFlux();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cancels the WebSocket connection subscription, stops the staleness checker,
     * and completes the sink (notifying all subscribers that the stream has ended).
     * The heartbeat is automatically cancelled because it runs within the session pipeline.</p>
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting Bybit connector");

        if (connectionDisposable != null && !connectionDisposable.isDisposed()) {
            connectionDisposable.dispose();
        }

        if (stalenessDisposable != null && !stalenessDisposable.isDisposed()) {
            stalenessDisposable.dispose();
        }

        sink.tryEmitComplete();
        feedStatus.set(FeedStatus.DISCONNECTED);
        metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.DISCONNECTED);
        lastMessageNanoTime.set(0);

        log.info("Bybit connector disconnected");
    }

    /** {@inheritDoc} */
    @Override
    public ExchangeId getExchangeId() {
        return ExchangeId.BYBIT;
    }

    /** {@inheritDoc} */
    @Override
    public FeedStatus getFeedStatus() {
        return feedStatus.get();
    }

    /**
     * Handles an individual WebSocket session: sends the subscription message,
     * starts the heartbeat, and processes all incoming messages.
     *
     * <p><b>Heartbeat design:</b> Uses {@link Flux#concat(org.reactivestreams.Publisher[])}
     * to create a single outbound stream: the subscription message is sent first, then
     * periodic ping messages every 20 seconds. This approach ensures:</p>
     * <ul>
     *   <li>Subscription always goes first (ordering guaranteed by concat)</li>
     *   <li>Single {@code session.send()} call (some WebSocket implementations reject
     *       multiple send publishers)</li>
     *   <li>Heartbeat is scoped to session lifetime — automatically cancelled on
     *       disconnect, automatically restarted on reconnection (new session = new
     *       handleSession call)</li>
     * </ul>
     *
     * <p>{@link Mono#when(org.reactivestreams.Publisher...)} runs send and receive
     * concurrently — WebSocket is full-duplex.</p>
     *
     * @param session     the WebSocket session
     * @param tradingPair the pair we're subscribing to
     * @return a Mono that completes when the session closes
     */
    private Mono<Void> handleSession(WebSocketSession session, TradingPair tradingPair) {
        feedStatus.set(FeedStatus.CONNECTED);
        metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.CONNECTED);
        // Reset stale timestamp from any previous session so the staleness checker
        // does not immediately fire before the first message of the new session arrives.
        lastMessageNanoTime.set(0);

        // Build subscription for all configured pairs; fall back to single pair in test mode
        List<TradingPair> allPairs = tradingPairsProperties != null
                ? tradingPairsProperties.asTradingPairs()
                : List.of(tradingPair);

        String args = allPairs.stream()
                .map(p -> "\"orderbook.1." + toBybitSymbol(p) + "\"")
                .collect(Collectors.joining(","));
        String subscriptionMessage = "{\"req_id\":\"1\",\"op\":\"subscribe\",\"args\":[" + args + "]}";

        String pairNames = allPairs.stream().map(TradingPair::canonicalSymbol)
                .collect(Collectors.joining(", "));
        log.info("Sending Bybit subscription: pairs=[{}]", pairNames);

        // Outbound: subscription message first, then periodic heartbeat pings.
        // Flux.concat ensures subscription is sent before any pings start.
        Flux<WebSocketMessage> outbound = Flux.concat(
                Mono.just(session.textMessage(subscriptionMessage)),
                Flux.interval(HEARTBEAT_INTERVAL)
                        .map(tick -> session.textMessage(PING_MESSAGE))
        );

        Mono<Void> send = session.send(outbound);

        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> processMessage(payload, tradingPair))
                .then();

        // Convert normal session completion to an error so retryWhen triggers reconnection.
        // Bybit closes WebSocket sessions with a graceful CLOSE frame (code 1000), which
        // causes Mono.when() to complete normally — bypassing retryWhen entirely. Wrapping
        // with .then(Mono.error(...)) ensures BOTH graceful closes AND TCP errors are retried.
        // On explicit disconnect(), the subscription is cancelled (not completed/errored),
        // so this does NOT cause spurious reconnects after manual disconnection.
        return Mono.when(send, receive)
                .then(Mono.error(new RuntimeException("Bybit WebSocket session closed - reconnecting")));
    }

    /**
     * Processes a single WebSocket message: captures T0, delegates parsing to
     * {@link BybitMessageParser}, and emits the result through the sink.
     *
     * <p><b>LATENCY CRITICAL:</b> {@code System.nanoTime()} is the very first operation
     * in this method — before JSON parsing, before logging, before anything. This is T0,
     * the ground truth for measuring pipeline latency. If you add any code before the
     * nanoTime() call, you corrupt every latency measurement downstream.</p>
     *
     * @param payload     the raw JSON string from the WebSocket
     * @param tradingPair the pair this message belongs to
     */
    void processMessage(String payload, TradingPair tradingPair) {
        // ============================================================
        // T0 — GROUND TRUTH TIMESTAMP
        // This MUST be the very first line. Do not add anything above.
        // ============================================================
        final long receivedNanos = System.nanoTime();

        lastMessageNanoTime.set(receivedNanos);
        metrics.recordMessageReceived(ExchangeId.BYBIT);

        // Recover from STALE if we were stale
        if (feedStatus.compareAndSet(FeedStatus.STALE, FeedStatus.CONNECTED)) {
            metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.CONNECTED);
        }

        Optional<NormalisedTick> maybeTick = messageParser.parse(payload, tradingPair, receivedNanos);

        maybeTick.ifPresent(tick -> {
            Sinks.EmitResult result = sink.tryEmitNext(tick);
            if (result.isFailure()) {
                log.warn("Failed to emit tick: result={} pair={}", result, tradingPair.canonicalSymbol());
            }
        });
    }

    /**
     * Starts a periodic staleness checker that monitors message freshness.
     *
     * <p>Every {@code stalenessThreshold} interval, checks if the time since the last
     * message exceeds the threshold. If so, transitions the feed status to
     * {@link FeedStatus#STALE}. When the next message arrives, {@link #processMessage}
     * transitions it back to {@link FeedStatus#CONNECTED}.</p>
     */
    private void startStalenessChecker() {
        Duration threshold = config.getStalenessThreshold();

        stalenessDisposable = Flux.interval(threshold)
                .subscribe(tick -> {
                    long lastMsg = lastMessageNanoTime.get();

                    // Don't check if we haven't received any messages yet, or if we're
                    // already reconnecting/disconnected
                    if (lastMsg == 0 || feedStatus.get() != FeedStatus.CONNECTED) {
                        return;
                    }

                    long elapsedNanos = System.nanoTime() - lastMsg;
                    if (elapsedNanos > threshold.toNanos()) {
                        FeedStatus previous = feedStatus.getAndSet(FeedStatus.STALE);
                        if (previous == FeedStatus.CONNECTED) {
                            metrics.recordFeedStatus(ExchangeId.BYBIT, FeedStatus.STALE);
                            log.warn("Bybit feed went STALE: noMessageFor={}ms threshold={}ms",
                                    elapsedNanos / 1_000_000, threshold.toMillis());
                        }
                    }
                });
    }

    /**
     * Converts a canonical trading pair to Bybit's WebSocket symbol format.
     *
     * <p>Bybit uses uppercase concatenated symbols: {@code BTC-USDT → BTCUSDT}.
     * This is different from Binance's lowercase format ({@code btcusdt}).</p>
     *
     * @param tradingPair the canonical trading pair
     * @return the Bybit-format symbol string (uppercase, no separator)
     */
    String toBybitSymbol(TradingPair tradingPair) {
        return (tradingPair.getBaseCurrency() + tradingPair.getQuoteCurrency()).toUpperCase();
    }
}
