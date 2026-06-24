package com.arbitrage.connector.binance;

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
 * Binance exchange connector that subscribes to the Book Ticker WebSocket stream
 * and emits {@link NormalisedTick} objects for each price update.
 *
 * <p><b>Book Ticker</b> provides the best bid and ask price/quantity for a symbol
 * in real-time. Binance pushes an update every time the best bid or ask changes —
 * typically hundreds of updates per second for active pairs like BTC-USDT.</p>
 *
 * <p><b>Latency discipline:</b> The very first operation in the message handler is
 * {@code System.nanoTime()} — this is T0, the ground truth timestamp for measuring
 * end-to-end pipeline latency. Everything else (parsing, logging, emission) happens
 * after T0 is captured.</p>
 *
 * <p><b>Feed health:</b> A {@link Flux#interval(Duration)} task monitors message
 * freshness. If no message arrives within the configured staleness threshold, the
 * feed status transitions from {@link FeedStatus#CONNECTED} to {@link FeedStatus#STALE}.
 * A stale feed is worse than a dead feed because it silently serves outdated data —
 * the detection engine must skip stale prices to avoid false positives.</p>
 *
 * <p><b>Reconnection:</b> Uses Reactor's {@link Retry#backoff(long, Duration)} for
 * automatic exponential backoff reconnection when the WebSocket connection drops.
 * The formal {@code ExponentialBackoffReconnectStrategy} class is created in Session 1.5;
 * this implementation uses Reactor's built-in retry operator.</p>
 *
 * @see ExchangeConnector for the Strategy Pattern interface
 * @see NormalisedTick for the output format
 */
@Component
@Slf4j
public class BinanceConnector implements ExchangeConnector {

    private final ExchangeConnectorProperties.ExchangeProperties config;
    private final BinanceMessageParser messageParser;
    private final TradingPairsProperties tradingPairsProperties;
    private final WebSocketClient webSocketClient;
    private final ExponentialBackoffReconnectStrategy reconnectStrategy;
    private final ConnectorMetrics metrics;

    /**
     * Hot sink that bridges imperative WebSocket callbacks into a reactive Flux.
     *
     * <p>{@code multicast()} means each subscriber gets only messages emitted after
     * they subscribe (no replay). {@code onBackpressureBuffer()} buffers if subscribers
     * are slow — acceptable for Phase 1; bounded buffer with overflow strategy can
     * be added in Phase 6 performance tuning.</p>
     */
    private final Sinks.Many<NormalisedTick> sink;

    /**
     * Current feed health status. Uses {@link AtomicReference} for thread-safe
     * transitions — the WebSocket event loop, staleness checker, and consumer
     * threads all read/write this concurrently.
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
     * Creates a Binance connector with configuration from application properties.
     *
     * @param properties             the exchange connector properties containing Binance config
     * @param messageParser          the parser for converting raw JSON to NormalisedTick
     * @param metrics                connector metrics for recording message rates, errors, and latency
     * @param tradingPairsProperties configured trading pairs used to build multi-pair subscription
     * @throws ExchangeConnectionException if Binance configuration is missing
     */
    @Autowired
    public BinanceConnector(ExchangeConnectorProperties properties,
                            BinanceMessageParser messageParser,
                            ConnectorMetrics metrics,
                            TradingPairsProperties tradingPairsProperties) {
        ExchangeConnectorProperties.ExchangeProperties binanceConfig =
                properties.getConnectors().get("binance");

        if (binanceConfig == null) {
            throw new ExchangeConnectionException(ExchangeId.BINANCE,
                    "Binance connector configuration is missing from arbitrage.exchanges.connectors.binance");
        }

        this.config = binanceConfig;
        this.messageParser = messageParser;
        this.tradingPairsProperties = tradingPairsProperties;
        this.webSocketClient = new ReactorNettyWebSocketClient();
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.BINANCE,
                binanceConfig.getInitialReconnectDelay(),
                binanceConfig.getMaxReconnectDelay(),
                binanceConfig.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);

        log.info("Initialised {}", reconnectStrategy);
    }

    /**
     * Package-private constructor for testing — allows injecting a mock WebSocketClient and metrics.
     *
     * @param config          Binance-specific exchange properties
     * @param messageParser   the parser for converting raw JSON to NormalisedTick
     * @param webSocketClient WebSocket client implementation (real or mock)
     * @param metrics         connector metrics (real or test SimpleMeterRegistry)
     */
    BinanceConnector(ExchangeConnectorProperties.ExchangeProperties config,
                     BinanceMessageParser messageParser,
                     WebSocketClient webSocketClient,
                     ConnectorMetrics metrics) {
        this.config = config;
        this.messageParser = messageParser;
        this.tradingPairsProperties = null; // tests use single-pair mode
        this.webSocketClient = webSocketClient;
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.BINANCE,
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
     * <p>Opens a WebSocket connection to Binance and subscribes to the bookTicker
     * stream for the given trading pair. The returned Flux emits {@link NormalisedTick}
     * objects in real-time as Binance pushes price updates.</p>
     *
     * <p>If already connected, returns the existing Flux without reconnecting.</p>
     *
     * @param tradingPair the pair to subscribe to (e.g., BTC-USDT)
     * @return a hot Flux of normalised ticks
     */
    @Override
    public Flux<NormalisedTick> connect(TradingPair tradingPair) {
        if (feedStatus.get() != FeedStatus.DISCONNECTED) {
            log.warn("BinanceConnector already active, status={}. Returning existing Flux.", feedStatus.get());
            return sink.asFlux();
        }

        if (!config.isEnabled()) {
            log.info("Binance connector is disabled in configuration. Not connecting.");
            return Flux.empty();
        }

        URI wsUri = URI.create(config.getWsEndpoint());
        log.info("Connecting to Binance WebSocket: endpoint={} pair={}", wsUri, tradingPair.canonicalSymbol());

        startStalenessChecker();

        connectionDisposable = webSocketClient.execute(wsUri,
                        session -> handleSession(session, tradingPair))
                .doOnSubscribe(subscription -> {
                    log.info("Binance WebSocket subscription started for pair={}", tradingPair.canonicalSymbol());
                })
                .doOnError(error -> {
                    log.error("Binance WebSocket error: pair={} error={}", tradingPair.canonicalSymbol(),
                            error.getMessage());
                })
                .doOnTerminate(() -> {
                    log.warn("Binance WebSocket terminated for pair={}", tradingPair.canonicalSymbol());
                })
                .retryWhen(reconnectStrategy.buildRetrySpec()
                        .doBeforeRetry(signal -> {
                            feedStatus.set(FeedStatus.RECONNECTING);
                            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.RECONNECTING);
                            metrics.recordReconnection(ExchangeId.BINANCE);
                        }))
                .doFinally(signalType -> {
                    feedStatus.set(FeedStatus.DISCONNECTED);
                    metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.DISCONNECTED);
                    log.info("Binance WebSocket connection finalised: signal={}", signalType);
                })
                .subscribe(
                        unused -> { },
                        error -> {
                            log.error("Binance connection failed permanently: {}", error.getMessage());
                            sink.tryEmitError(new ExchangeConnectionException(
                                    ExchangeId.BINANCE, error.getMessage(), error));
                        }
                );

        return sink.asFlux();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cancels the WebSocket connection subscription, stops the staleness checker,
     * and completes the sink (notifying all subscribers that the stream has ended).</p>
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting Binance connector");

        if (connectionDisposable != null && !connectionDisposable.isDisposed()) {
            connectionDisposable.dispose();
        }

        if (stalenessDisposable != null && !stalenessDisposable.isDisposed()) {
            stalenessDisposable.dispose();
        }

        sink.tryEmitComplete();
        feedStatus.set(FeedStatus.DISCONNECTED);
        metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.DISCONNECTED);
        lastMessageNanoTime.set(0);

        log.info("Binance connector disconnected");
    }

    /** {@inheritDoc} */
    @Override
    public ExchangeId getExchangeId() {
        return ExchangeId.BINANCE;
    }

    /** {@inheritDoc} */
    @Override
    public FeedStatus getFeedStatus() {
        return feedStatus.get();
    }

    /**
     * Handles an individual WebSocket session: sends the subscription message,
     * then processes all incoming messages.
     *
     * <p>Uses {@link Mono#when(org.reactivestreams.Publisher...)} to run send and
     * receive concurrently. WebSocket is full-duplex — send and receive are independent
     * streams on the same connection. Using {@code send.then(receive)} would work but
     * delays the receive stream until send completes; {@code Mono.when()} starts both
     * immediately.</p>
     *
     * @param session     the WebSocket session
     * @param tradingPair the pair we're subscribing to
     * @return a Mono that completes when the session closes
     */
    private Mono<Void> handleSession(WebSocketSession session, TradingPair tradingPair) {
        feedStatus.set(FeedStatus.CONNECTED);
        metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.CONNECTED);
        lastMessageNanoTime.set(0);

        // Build subscription for all configured pairs; fall back to single pair in test mode
        List<TradingPair> allPairs = tradingPairsProperties != null
                ? tradingPairsProperties.asTradingPairs()
                : List.of(tradingPair);

        String params = allPairs.stream()
                .map(p -> "\"" + toBinanceSymbol(p) + "@bookTicker\"")
                .collect(Collectors.joining(","));
        String subscriptionMessage = "{\"method\":\"SUBSCRIBE\",\"params\":[" + params + "],\"id\":1}";

        String pairNames = allPairs.stream().map(TradingPair::canonicalSymbol)
                .collect(Collectors.joining(", "));
        log.info("Sending Binance subscription: pairs=[{}]", pairNames);

        Mono<Void> send = session.send(
                Mono.just(session.textMessage(subscriptionMessage)));

        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> processMessage(payload, tradingPair))
                .then();

        return Mono.when(send, receive)
                .then(Mono.error(new RuntimeException("Binance WebSocket session closed - reconnecting")));
    }

    /**
     * Processes a single WebSocket message: captures T0, delegates parsing to
     * {@link BinanceMessageParser}, and emits the result through the sink.
     *
     * <p><b>LATENCY CRITICAL:</b> {@code System.nanoTime()} is the very first operation
     * in this method — before JSON parsing, before logging, before anything. This is T0,
     * the ground truth for measuring pipeline latency. If you add any code before the
     * nanoTime() call, you corrupt every latency measurement downstream.</p>
     *
     * <p><b>Responsibility split:</b> This method handles connection concerns (T0 capture,
     * staleness recovery, sink emission). The parser handles data transformation (JSON →
     * NormalisedTick). Different reasons to change → different classes (SRP).</p>
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
        metrics.recordMessageReceived(ExchangeId.BINANCE);

        // Recover from STALE if we were stale
        if (feedStatus.compareAndSet(FeedStatus.STALE, FeedStatus.CONNECTED)) {
            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.CONNECTED);
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
     *
     * <p><b>Why not check on each message arrival?</b> Staleness means "no messages are
     * arriving." You cannot detect the absence of an event by handling events — you need
     * a periodic check. This is the same pattern used in TCP keepalive and WebSocket
     * heartbeat protocols.</p>
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
                            metrics.recordFeedStatus(ExchangeId.BINANCE, FeedStatus.STALE);
                            log.warn("Binance feed went STALE: noMessageFor={}ms threshold={}ms",
                                    elapsedNanos / 1_000_000, threshold.toMillis());
                        }
                    }
                });
    }

    /**
     * Converts a canonical trading pair to Binance's WebSocket symbol format.
     *
     * <p>Binance uses lowercase concatenated symbols: {@code BTC-USDT → btcusdt}.
     * This is Binance-specific — Bybit uses {@code BTCUSDT}, KuCoin uses {@code BTC-USDT}.</p>
     *
     * @param tradingPair the canonical trading pair
     * @return the Binance-format symbol string
     */
    String toBinanceSymbol(TradingPair tradingPair) {
        return (tradingPair.getBaseCurrency() + tradingPair.getQuoteCurrency()).toLowerCase();
    }

}
