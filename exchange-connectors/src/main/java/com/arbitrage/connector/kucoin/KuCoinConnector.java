package com.arbitrage.connector.kucoin;

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
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
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
 * KuCoin exchange connector that subscribes to the {@code /market/ticker} WebSocket stream
 * and emits {@link NormalisedTick} objects for each price update.
 *
 * <p><b>Two-step connection protocol:</b> Unlike Binance and Bybit, KuCoin requires a REST
 * bootstrap before the WebSocket can be opened:
 * <ol>
 *   <li>POST to {@code https://api.kucoin.com/api/v1/bullet-public} via
 *       {@link KuCoinTokenService} to get a token and WebSocket endpoint URL</li>
 *   <li>Open WebSocket to {@code {endpoint}?token={TOKEN}&connectId={UUID}}</li>
 * </ol>
 * This REST call is handled reactively — the connection flow is:
 * {@code tokenService.getConnectionDetails().flatMap(details -> webSocketClient.execute(...))}
 *
 * <p><b>Symbol format (a welcome surprise):</b> KuCoin uses {@code BTC-USDT}
 * (uppercase, hyphen-separated) in its subscription topics. This matches our canonical
 * symbol format exactly. No symbol conversion method is needed — {@code tradingPair.canonicalSymbol()}
 * can be passed directly to the KuCoin topic. This is a contrast to Binance ({@code btcusdt})
 * and Bybit ({@code BTCUSDT}) which both require transformation.</p>
 *
 * <p><b>Heartbeat:</b> KuCoin disconnects the client if no ping is received within
 * {@code pingInterval} milliseconds (typically 18 seconds, from the bullet-public response).
 * The client sends {@code {"id":"ping-N","type":"ping"}} and the server responds with
 * {@code {"id":"ping-N","type":"pong"}}. Unlike Bybit, the heartbeat interval is not
 * hardcoded — it comes from the API response and is passed through to the session handler.</p>
 *
 * <p><b>Welcome message:</b> KuCoin sends {@code {"id":"...","type":"welcome"}} as the
 * first message after connection. This confirms that the token was accepted. If the welcome
 * is not received, the WebSocket was rejected at the protocol level.</p>
 *
 * <p><b>Token refresh on reconnect:</b> {@link KuCoinTokenService#invalidateCache()} is called
 * before each reconnection attempt. This ensures a fresh token is fetched rather than reusing
 * a potentially-expired one. The cost is one extra REST call (~50–200ms) per reconnect, which
 * is acceptable given reconnects are rare events.</p>
 *
 * <p><b>Latency discipline:</b> {@code System.nanoTime()} is captured as the very first
 * operation in {@link #processMessage} — T0. No code may precede this call.</p>
 *
 * @see ExchangeConnector for the Strategy Pattern interface
 * @see KuCoinTokenService for the REST token bootstrap
 * @see KuCoinMessageParser for JSON parsing logic
 */
@Component
@Slf4j
public class KuCoinConnector implements ExchangeConnector {

    /**
     * KuCoin heartbeat ping message template.
     * The {@code id} is a monotonically increasing sequence number within each session.
     * KuCoin responds with {@code {"id":"ping-N","type":"pong"}}.
     */
    private static final String PING_TEMPLATE =
            "{\"id\":\"ping-%d\",\"type\":\"ping\"}";

    private final ExchangeConnectorProperties.ExchangeProperties config;
    private final KuCoinTokenService tokenService;
    private final KuCoinMessageParser messageParser;
    private final TradingPairsProperties tradingPairsProperties;
    private final WebSocketClient webSocketClient;
    private final ExponentialBackoffReconnectStrategy reconnectStrategy;
    private final ConnectorMetrics metrics;

    /**
     * Hot sink bridging imperative WebSocket callbacks into a reactive Flux.
     * Multicast — each subscriber receives messages emitted after their subscription.
     */
    private final Sinks.Many<NormalisedTick> sink;

    /**
     * Current feed health status. AtomicReference for thread-safe transitions
     * across the WebSocket event loop, staleness checker, and heartbeat threads.
     */
    private final AtomicReference<FeedStatus> feedStatus;

    /**
     * Nanosecond timestamp of the last received WebSocket message.
     * Used by the staleness checker. Zero means no message received yet.
     */
    private final AtomicLong lastMessageNanoTime;

    /** Subscription to the WebSocket connection + retry loop. Cancelled on disconnect. */
    private volatile Disposable connectionDisposable;

    /** Subscription to the staleness checker interval. Cancelled on disconnect. */
    private volatile Disposable stalenessDisposable;

    /**
     * Creates a KuCoin connector with configuration from application properties.
     *
     * @param properties             the exchange connector properties containing KuCoin config
     * @param tokenService           the service for obtaining REST-bootstrapped WS tokens
     * @param messageParser          the parser for converting raw JSON to NormalisedTick
     * @param metrics                connector metrics for message rates, errors, and latency
     * @param tradingPairsProperties configured trading pairs used to build multi-pair subscription
     * @throws ExchangeConnectionException if KuCoin configuration is missing
     */
    @Autowired
    public KuCoinConnector(ExchangeConnectorProperties properties,
                           KuCoinTokenService tokenService,
                           KuCoinMessageParser messageParser,
                           ConnectorMetrics metrics,
                           TradingPairsProperties tradingPairsProperties) {
        ExchangeConnectorProperties.ExchangeProperties kucoinConfig =
                properties.getConnectors().get("kucoin");

        if (kucoinConfig == null) {
            throw new ExchangeConnectionException(ExchangeId.KUCOIN,
                    "KuCoin connector configuration is missing from arbitrage.exchanges.connectors.kucoin");
        }

        this.config = kucoinConfig;
        this.tokenService = tokenService;
        this.messageParser = messageParser;
        this.tradingPairsProperties = tradingPairsProperties;
        // Force HTTP/1.1 — KuCoin's Cloudflare layer resets TLS connections that negotiate
        // HTTP/2 via ALPN (Reactor-Netty's default). WebSocket runs over HTTP/1.1 anyway;
        // disabling ALPN removes the fingerprinting that triggers the Cloudflare reset.
        HttpClient httpClient = HttpClient.create().protocol(HttpProtocol.HTTP11);
        this.webSocketClient = new ReactorNettyWebSocketClient(httpClient);
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.KUCOIN,
                kucoinConfig.getInitialReconnectDelay(),
                kucoinConfig.getMaxReconnectDelay(),
                kucoinConfig.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);

        log.info("Initialised {}", reconnectStrategy);
    }

    /**
     * Package-private constructor for testing — allows injecting mock dependencies.
     *
     * @param config          KuCoin-specific exchange properties
     * @param tokenService    mock or stub token service
     * @param messageParser   the parser for converting raw JSON to NormalisedTick
     * @param webSocketClient WebSocket client implementation (real or mock)
     * @param metrics         connector metrics
     */
    KuCoinConnector(ExchangeConnectorProperties.ExchangeProperties config,
                    KuCoinTokenService tokenService,
                    KuCoinMessageParser messageParser,
                    WebSocketClient webSocketClient,
                    ConnectorMetrics metrics) {
        this.config = config;
        this.tokenService = tokenService;
        this.messageParser = messageParser;
        this.tradingPairsProperties = null; // tests use single-pair mode
        this.webSocketClient = webSocketClient;
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.KUCOIN,
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
     * <p>Executes the two-step KuCoin connection protocol:
     * <ol>
     *   <li>Obtains a WS token via {@link KuCoinTokenService} (REST call, cached)</li>
     *   <li>Opens a WebSocket to {@code {endpoint}?token={TOKEN}&connectId={UUID}}</li>
     * </ol>
     *
     * <p>The connection is established asynchronously — this method returns immediately.
     * The returned Flux starts emitting ticks as they arrive from KuCoin.</p>
     *
     * <p>On reconnection, the token cache is invalidated before each retry so a fresh
     * token is fetched. This prevents auth failures from reusing an expired token.</p>
     */
    @Override
    public Flux<NormalisedTick> connect(TradingPair tradingPair) {
        if (feedStatus.get() != FeedStatus.DISCONNECTED) {
            log.warn("KuCoinConnector already active, status={}. Returning existing Flux.", feedStatus.get());
            return sink.asFlux();
        }

        if (!config.isEnabled()) {
            log.info("KuCoin connector is disabled in configuration. Not connecting.");
            return Flux.empty();
        }

        log.info("Starting KuCoin connection for pair={}", tradingPair.canonicalSymbol());

        startStalenessChecker();

        connectionDisposable = tokenService.getConnectionDetails()
                .flatMap(details -> {
                    // Construct the authenticated WebSocket URL.
                    // connectId is a client-generated identifier for logging/debugging on KuCoin's side.
                    String connectId = Long.toHexString(System.nanoTime());
                    URI wsUri = URI.create(details.wsEndpoint()
                            + "?token=" + details.token()
                            + "&connectId=" + connectId);

                    log.info("Connecting to KuCoin WebSocket: endpoint={} pair={}",
                            details.wsEndpoint(), tradingPair.canonicalSymbol());

                    return webSocketClient.execute(wsUri,
                            session -> handleSession(session, tradingPair, details.pingIntervalMs()));
                })
                .doOnSubscribe(subscription ->
                        log.info("KuCoin WebSocket subscription started for pair={}", tradingPair.canonicalSymbol()))
                .doOnError(error ->
                        log.error("KuCoin WebSocket error: pair={} error={}",
                                tradingPair.canonicalSymbol(), error.getMessage()))
                .doOnTerminate(() ->
                        log.warn("KuCoin WebSocket terminated for pair={}", tradingPair.canonicalSymbol()))
                .retryWhen(reconnectStrategy.buildRetrySpec()
                        .doBeforeRetry(signal -> {
                            // Invalidate the token cache before each reconnect.
                            // A reconnect after a long connection drop may be caused by token expiry.
                            // Fetching a fresh token costs one REST round-trip but eliminates
                            // the risk of a permanent auth failure loop.
                            tokenService.invalidateCache();
                            feedStatus.set(FeedStatus.RECONNECTING);
                            metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.RECONNECTING);
                            metrics.recordReconnection(ExchangeId.KUCOIN);
                        }))
                .doFinally(signalType -> {
                    feedStatus.set(FeedStatus.DISCONNECTED);
                    metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.DISCONNECTED);
                    log.info("KuCoin WebSocket connection finalised: signal={}", signalType);
                })
                .subscribe(
                        unused -> { },
                        error -> {
                            log.error("KuCoin connection failed permanently: {}", error.getMessage());
                            sink.tryEmitError(new ExchangeConnectionException(
                                    ExchangeId.KUCOIN, error.getMessage(), error));
                        }
                );

        return sink.asFlux();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cancels the WebSocket connection and staleness checker, completes the sink,
     * and invalidates the token cache so the next {@link #connect} call fetches a fresh token.</p>
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting KuCoin connector");

        if (connectionDisposable != null && !connectionDisposable.isDisposed()) {
            connectionDisposable.dispose();
        }

        if (stalenessDisposable != null && !stalenessDisposable.isDisposed()) {
            stalenessDisposable.dispose();
        }

        sink.tryEmitComplete();
        feedStatus.set(FeedStatus.DISCONNECTED);
        metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.DISCONNECTED);
        lastMessageNanoTime.set(0);
        tokenService.invalidateCache();

        log.info("KuCoin connector disconnected");
    }

    /** {@inheritDoc} */
    @Override
    public ExchangeId getExchangeId() {
        return ExchangeId.KUCOIN;
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
     * <p><b>Heartbeat design:</b> Identical to Bybit — {@link Flux#concat} ensures the
     * subscription is sent first, then periodic pings follow at {@code pingIntervalMs}
     * intervals. The ping interval comes from the bullet-public response (not hardcoded)
     * and is session-scoped so it stops automatically on disconnect.</p>
     *
     * <p><b>First message sequence:</b> KuCoin sends a welcome message before any ticker
     * data. The parser logs this at INFO level. The subscribe message is sent immediately
     * after the welcome arrives (from KuCoin's perspective) because we send it as part of
     * the outbound flux before any pings — {@code Flux.concat} guarantees ordering.</p>
     *
     * @param session        the WebSocket session
     * @param tradingPair    the pair to subscribe to
     * @param pingIntervalMs heartbeat interval from the bullet-public response
     * @return a Mono that completes when the session closes
     */
    private Mono<Void> handleSession(WebSocketSession session, TradingPair tradingPair, long pingIntervalMs) {
        feedStatus.set(FeedStatus.CONNECTED);
        metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.CONNECTED);
        lastMessageNanoTime.set(0);

        // Build subscription for all configured pairs; fall back to single pair in test mode.
        // KuCoin supports comma-separated pairs in one topic: /market/ticker:BTC-USDT,ETH-USDT,BNB-USDT
        List<TradingPair> allPairs = tradingPairsProperties != null
                ? tradingPairsProperties.asTradingPairs()
                : List.of(tradingPair);

        String topicPairs = allPairs.stream()
                .map(TradingPair::canonicalSymbol)
                .collect(Collectors.joining(","));
        String subscriptionMessage = "{\"id\":\"sub-1\",\"type\":\"subscribe\",\"topic\":\"/market/ticker:"
                + topicPairs + "\",\"privateChannel\":false,\"response\":true}";

        String pairNames = allPairs.stream().map(TradingPair::canonicalSymbol)
                .collect(Collectors.joining(", "));
        log.info("Sending KuCoin subscription: pairs=[{}]", pairNames);

        // Outbound: subscription first, then periodic heartbeat pings.
        // Flux.concat guarantees subscription is sent before pings start.
        // Ping IDs use the interval tick count (i) as a monotonically increasing sequence.
        Flux<WebSocketMessage> outbound = Flux.concat(
                Mono.just(session.textMessage(subscriptionMessage)),
                Flux.interval(Duration.ofMillis(pingIntervalMs))
                        .map(i -> session.textMessage(String.format(PING_TEMPLATE, i)))
        );

        Mono<Void> send = session.send(outbound);

        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> processMessage(payload, tradingPair))
                .then();

        return Mono.when(send, receive)
                .then(Mono.error(new RuntimeException("KuCoin WebSocket session closed - reconnecting")));
    }

    /**
     * Processes a single WebSocket message: captures T0, delegates parsing to
     * {@link KuCoinMessageParser}, and emits the result through the sink.
     *
     * <p><b>LATENCY CRITICAL:</b> {@code System.nanoTime()} MUST be the very first
     * operation in this method — before JSON parsing, logging, or anything else.
     * This is T0, the ground truth for measuring pipeline latency. Any code added
     * before the {@code nanoTime()} call corrupts all latency measurements.</p>
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
        metrics.recordMessageReceived(ExchangeId.KUCOIN);

        // Recover from STALE if we were stale
        if (feedStatus.compareAndSet(FeedStatus.STALE, FeedStatus.CONNECTED)) {
            metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.CONNECTED);
        }

        Optional<NormalisedTick> maybeTick = messageParser.parse(payload, tradingPair, receivedNanos);

        maybeTick.ifPresent(tick -> {
            Sinks.EmitResult result = sink.tryEmitNext(tick);
            if (result.isFailure()) {
                log.warn("Failed to emit KuCoin tick: result={} pair={}", result, tradingPair.canonicalSymbol());
            }
        });
    }

    /**
     * Starts a periodic staleness checker that monitors message freshness.
     *
     * <p>If no message arrives within the configured staleness threshold, the feed
     * transitions to {@link FeedStatus#STALE}. When the next message arrives,
     * {@link #processMessage} transitions it back to {@link FeedStatus#CONNECTED}.</p>
     */
    private void startStalenessChecker() {
        Duration threshold = config.getStalenessThreshold();

        stalenessDisposable = Flux.interval(threshold)
                .subscribe(tick -> {
                    long lastMsg = lastMessageNanoTime.get();

                    if (lastMsg == 0 || feedStatus.get() != FeedStatus.CONNECTED) {
                        return;
                    }

                    long elapsedNanos = System.nanoTime() - lastMsg;
                    if (elapsedNanos > threshold.toNanos()) {
                        FeedStatus previous = feedStatus.getAndSet(FeedStatus.STALE);
                        if (previous == FeedStatus.CONNECTED) {
                            metrics.recordFeedStatus(ExchangeId.KUCOIN, FeedStatus.STALE);
                            log.warn("KuCoin feed went STALE: noMessageFor={}ms threshold={}ms",
                                    elapsedNanos / 1_000_000, threshold.toMillis());
                        }
                    }
                });
    }
}
