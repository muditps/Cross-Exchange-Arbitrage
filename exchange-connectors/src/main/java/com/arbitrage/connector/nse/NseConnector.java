package com.arbitrage.connector.nse;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnectionException;
import com.arbitrage.connector.ExchangeConnector;
import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.arbitrage.connector.reconnect.ExponentialBackoffReconnectStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * NSE (National Stock Exchange of India) connector via Angel One SmartAPI SmartStream.
 *
 * <p><b>Three-step connection protocol</b> (more complex than any crypto connector):
 * <ol>
 *   <li>Generate a fresh TOTP code from the configured base32 secret.</li>
 *   <li>POST to Angel One's login endpoint to exchange credentials for
 *       {@code jwtToken} + {@code feedToken} (via {@link NseTokenService}).</li>
 *   <li>Open WebSocket to {@code wss://smartapisocket.angelone.in/smart-stream} with
 *       {@code Authorization}, {@code x-feed-token}, and {@code x-client-code} headers.</li>
 * </ol>
 *
 * <p><b>Instrument tokens, not symbols:</b> Angel One identifies instruments by numeric
 * token (e.g., RELIANCE = 2885, TCS = 11536) in both subscription and response messages.
 * The subscription message sends a list of tokens; the response tags each tick with the token.
 * {@link NseConnectorProperties#getInstruments()} provides the canonical-pair → token mapping.
 * The reverse (token → pair) lookup is handled in {@link NseMessageParser} (Session 7A.2).
 *
 * <p><b>Subscription mode 2 (Quote):</b> Provides best-bid/ask from the top of the order
 * book ({@code best_5_buy_data[0]} and {@code best_5_sell_data[0]}). Mode 1 (LTP) is
 * insufficient for arbitrage — it gives the last traded price, not the current spread.
 * Mode 3 (SnapQuote) provides full depth but is higher bandwidth than needed.
 *
 * <p><b>Price unit — paise:</b> Angel One transmits all prices as integers in paise
 * (1 paisa = ₹0.01). RELIANCE at ₹2450.00 is sent as {@code 245000}.
 * {@link NseMessageParser} divides by 100 and converts to {@code BigDecimal} (Session 7A.2).
 *
 * <p><b>Market hours awareness:</b> NSE cash market trades 09:15–15:30 IST, Monday–Friday.
 * Outside these hours, no ticks arrive. The staleness threshold is set to 60 seconds
 * (vs 500 ms for crypto) so the feed does not flip to {@link FeedStatus#STALE} during
 * normal quiet periods like auction phases or circuit breakers.
 *
 * <p><b>No TickKafkaProducer in 7A.1:</b> {@code NseConnector} is wired into
 * {@link com.arbitrage.connector.registry.ExchangeConnectorRegistry} automatically via
 * Spring list injection and appears in feed-status health checks as DISCONNECTED.
 * {@code NseTickKafkaProducer} (which triggers {@link #connect}) is added in Session 7A.3
 * after the parser is complete.
 *
 * @see NseTokenService for the authentication bootstrap
 * @see NseMessageParser for message parsing (implemented in Session 7A.2)
 * @see NseConnectorProperties for instrument token configuration
 */
@Component
@Slf4j
public class NseConnector implements ExchangeConnector {

    /**
     * Angel One SmartStream heartbeat ping message.
     * Server closes connection if no ping received within ~60 seconds.
     * Simple string "ping" (not JSON) per Angel One SmartAPI documentation.
     */
    private static final String HEARTBEAT_PING = "ping";

    /**
     * Angel One exchange type identifier for NSE cash market segment.
     * Used in the subscription JSON {@code tokenList[].exchangeType} field.
     * BSE cash market uses type 3.
     */
    private static final int EXCHANGE_TYPE_NSE = 1;

    /**
     * Subscription mode for Quote data (best bid + best ask from order book top).
     * Mode 1 = LTP only (insufficient for arbitrage).
     * Mode 2 = Quote (bid/ask — what we need).
     * Mode 3 = SnapQuote (full depth, more bandwidth than required).
     */
    private static final int SUBSCRIPTION_MODE_QUOTE = 2;

    private final ExchangeConnectorProperties.ExchangeProperties config;
    private final NseConnectorProperties nseProps;
    private final NseTokenService tokenService;
    private final NseMessageParser messageParser;
    private final WebSocketClient webSocketClient;
    private final ExponentialBackoffReconnectStrategy reconnectStrategy;
    private final ConnectorMetrics metrics;

    /** Hot sink bridging WebSocket callbacks into a reactive Flux. */
    private final Sinks.Many<NormalisedTick> sink;

    /** Current feed health status; AtomicReference for thread-safe transitions. */
    private final AtomicReference<FeedStatus> feedStatus;

    /** Nanosecond timestamp of last received WebSocket message. Zero = none yet. */
    private final AtomicLong lastMessageNanoTime;

    /** Subscription to the WebSocket + retry loop. Cancelled on disconnect. */
    private volatile Disposable connectionDisposable;

    /** Subscription to the staleness checker interval. Cancelled on disconnect. */
    private volatile Disposable stalenessDisposable;

    /**
     * Creates the NSE connector with all dependencies from the Spring application context.
     *
     * @param connectorProps global exchange connector properties (ws-endpoint, staleness, etc.)
     * @param nseProps       NSE-specific properties (auth fields, instrument map)
     * @param tokenService   service that handles Angel One authentication
     * @param messageParser  parser for Angel One SmartStream JSON messages
     * @param metrics        connector metrics for message rates, errors, and latency
     * @throws ExchangeConnectionException if NSE connector configuration is missing
     */
    @Autowired
    public NseConnector(ExchangeConnectorProperties connectorProps,
                        NseConnectorProperties nseProps,
                        NseTokenService tokenService,
                        NseMessageParser messageParser,
                        ConnectorMetrics metrics) {
        ExchangeConnectorProperties.ExchangeProperties nseConfig =
                connectorProps.getConnectors().get("nse");

        if (nseConfig == null) {
            throw new ExchangeConnectionException(ExchangeId.NSE,
                    "NSE connector configuration is missing from arbitrage.exchanges.connectors.nse");
        }

        this.config = nseConfig;
        this.nseProps = nseProps;
        this.tokenService = tokenService;
        this.messageParser = messageParser;
        this.webSocketClient = new ReactorNettyWebSocketClient();
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.NSE,
                nseConfig.getInitialReconnectDelay(),
                nseConfig.getMaxReconnectDelay(),
                nseConfig.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);

        log.info("Initialised {}", reconnectStrategy);
    }

    /**
     * Package-private constructor for testing — allows injecting mock dependencies.
     *
     * @param config          NSE-specific exchange properties (ws-endpoint, etc.)
     * @param nseProps        NSE connector properties (auth fields, instruments)
     * @param tokenService    mock or stub token service
     * @param messageParser   the parser for converting raw JSON to NormalisedTick
     * @param webSocketClient WebSocket client implementation (real or mock)
     * @param metrics         connector metrics
     */
    NseConnector(ExchangeConnectorProperties.ExchangeProperties config,
                 NseConnectorProperties nseProps,
                 NseTokenService tokenService,
                 NseMessageParser messageParser,
                 WebSocketClient webSocketClient,
                 ConnectorMetrics metrics) {
        this.config = config;
        this.nseProps = nseProps;
        this.tokenService = tokenService;
        this.messageParser = messageParser;
        this.webSocketClient = webSocketClient;
        this.reconnectStrategy = new ExponentialBackoffReconnectStrategy(
                ExchangeId.NSE,
                config.getInitialReconnectDelay(),
                config.getMaxReconnectDelay(),
                config.getMaxReconnectAttempts());
        this.metrics = metrics;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.feedStatus = new AtomicReference<>(FeedStatus.DISCONNECTED);
        this.lastMessageNanoTime = new AtomicLong(0);
    }

    /** {@inheritDoc} */
    @Override
    public ExchangeId getExchangeId() {
        return ExchangeId.NSE;
    }

    /** {@inheritDoc} */
    @Override
    public FeedStatus getFeedStatus() {
        return feedStatus.get();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Executes the three-step NSE connection protocol:
     * <ol>
     *   <li>Authenticate with Angel One via {@link NseTokenService} (REST call, cached).</li>
     *   <li>Open WebSocket with JWT + feed token in headers.</li>
     *   <li>Send mode-2 Quote subscription for all configured instruments.</li>
     * </ol>
     *
     * <p>The {@code tradingPair} parameter is the startup trigger used for logging.
     * The connector subscribes to ALL instruments configured in
     * {@link NseConnectorProperties#getInstruments()}, not just this single pair —
     * consistent with the multi-pair behaviour of the crypto connectors.</p>
     *
     * <p>If {@code enabled: false} in configuration, returns {@link Flux#empty()} immediately.
     * This is the default state — NSE requires credentials to be set in the environment.</p>
     */
    @Override
    public Flux<NormalisedTick> connect(TradingPair tradingPair) {
        if (feedStatus.get() != FeedStatus.DISCONNECTED) {
            log.warn("NseConnector already active, status={}. Returning existing Flux.", feedStatus.get());
            return sink.asFlux();
        }

        if (!config.isEnabled()) {
            log.info("NSE connector is disabled (NSE_ENABLED=false). Set credentials and enable to connect.");
            return Flux.empty();
        }

        if (nseProps.getInstruments().isEmpty()) {
            log.warn("NSE connector enabled but no instruments configured under arbitrage.nse.instruments. Not connecting.");
            return Flux.empty();
        }

        log.info("Starting NSE connection: instruments={}", nseProps.getInstruments().keySet());
        startStalenessChecker();

        connectionDisposable = tokenService.getCredentials()
                .flatMap(credentials -> {
                    URI wsUri = URI.create(config.getWsEndpoint());
                    log.info("Connecting to Angel One SmartStream: endpoint={} instruments={}",
                            wsUri, nseProps.getInstruments().keySet());
                    org.springframework.http.HttpHeaders wsHeaders = buildHeaders(credentials);
                    return webSocketClient.execute(wsUri, wsHeaders,
                            session -> handleSession(session, tradingPair, credentials));
                })
                .doOnSubscribe(s -> log.info("NSE WebSocket subscription started"))
                .doOnError(e -> log.error("NSE WebSocket error: {}", e.getMessage()))
                .doOnTerminate(() -> log.warn("NSE WebSocket terminated"))
                .retryWhen(reconnectStrategy.buildRetrySpec()
                        .doBeforeRetry(signal -> {
                            tokenService.invalidateCache();
                            feedStatus.set(FeedStatus.RECONNECTING);
                            metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.RECONNECTING);
                            metrics.recordReconnection(ExchangeId.NSE);
                        }))
                .doFinally(signalType -> {
                    feedStatus.set(FeedStatus.DISCONNECTED);
                    metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.DISCONNECTED);
                    log.info("NSE WebSocket connection finalised: signal={}", signalType);
                })
                .subscribe(
                        unused -> { },
                        error -> {
                            log.error("NSE connection failed permanently: {}", error.getMessage());
                            sink.tryEmitError(new ExchangeConnectionException(
                                    ExchangeId.NSE, error.getMessage(), error));
                        }
                );

        return sink.asFlux();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cancels the WebSocket subscription and staleness checker, completes the sink,
     * and invalidates the auth token cache so the next {@link #connect} call re-authenticates.</p>
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting NSE connector");

        if (connectionDisposable != null && !connectionDisposable.isDisposed()) {
            connectionDisposable.dispose();
        }
        if (stalenessDisposable != null && !stalenessDisposable.isDisposed()) {
            stalenessDisposable.dispose();
        }

        sink.tryEmitComplete();
        feedStatus.set(FeedStatus.DISCONNECTED);
        metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.DISCONNECTED);
        lastMessageNanoTime.set(0);
        tokenService.invalidateCache();

        log.info("NSE connector disconnected");
    }

    /**
     * Builds the WebSocket HTTP headers required by Angel One SmartStream.
     *
     * <p>Three headers are mandatory; absence of any one results in a 403 rejection:
     * <ul>
     *   <li>{@code Authorization} — "Bearer {jwtToken}" (includes the "Bearer " prefix
     *       as returned by Angel One in the login response)</li>
     *   <li>{@code x-feed-token} — separate SmartStream-specific token issued alongside JWT</li>
     *   <li>{@code x-client-code} — Angel One account client ID</li>
     * </ul>
     *
     * @param credentials the authenticated session credentials
     * @return populated HttpHeaders for the WebSocket upgrade request
     */
    private HttpHeaders buildHeaders(NseAuthCredentials credentials) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, credentials.jwtToken());
        headers.add("x-feed-token", credentials.feedToken());
        headers.add("x-client-code", credentials.clientCode());
        return headers;
    }

    /**
     * Handles an individual WebSocket session: sends the mode-2 Quote subscription for all
     * configured instruments, starts the heartbeat, and processes all incoming messages.
     *
     * <p><b>Subscription message format:</b>
     * <pre>{@code
     * {
     *   "correlationID": "nse-sub-1",
     *   "action": 1,
     *   "params": {
     *     "mode": 2,
     *     "tokenList": [{ "exchangeType": 1, "tokens": ["2885", "11536", "1594"] }]
     *   }
     * }
     * }</pre>
     *
     * <p><b>Heartbeat:</b> Angel One disconnects clients that do not send a ping within ~60 seconds.
     * We send the plain string {@code "ping"} every {@link NseConnectorProperties#getHeartbeatIntervalSeconds()}
     * seconds (default 30). The server responds with {@code "pong"} which {@link #processMessage}
     * discards after updating the liveness timestamp.</p>
     *
     * @param session     the WebSocket session
     * @param tradingPair the initial trigger pair (used for logging only)
     * @param credentials the auth credentials (clientCode used for logging)
     * @return a Mono that completes when the session closes
     */
    private Mono<Void> handleSession(WebSocketSession session, TradingPair tradingPair,
                                     NseAuthCredentials credentials) {
        feedStatus.set(FeedStatus.CONNECTED);
        metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.CONNECTED);
        lastMessageNanoTime.set(0);

        List<String> tokens = nseProps.getInstruments().entrySet().stream()
                .filter(e -> e.getKey().endsWith("-INR"))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        String tokenArray = tokens.stream()
                .map(t -> "\"" + t + "\"")
                .collect(Collectors.joining(","));

        String subscriptionMessage = "{\"correlationID\":\"nse-sub-1\",\"action\":1,"
                + "\"params\":{\"mode\":" + SUBSCRIPTION_MODE_QUOTE + ","
                + "\"tokenList\":[{\"exchangeType\":" + EXCHANGE_TYPE_NSE
                + ",\"tokens\":[" + tokenArray + "]}]}}";

        log.info("Sending NSE subscription: instruments={} tokens={}",
                nseProps.getInstruments().keySet(), tokens);

        Duration heartbeatInterval = Duration.ofSeconds(nseProps.getHeartbeatIntervalSeconds());

        Flux<WebSocketMessage> outbound = Flux.concat(
                Mono.just(session.textMessage(subscriptionMessage)),
                Flux.interval(heartbeatInterval).map(i -> session.textMessage(HEARTBEAT_PING))
        );

        Mono<Void> send = session.send(outbound);
        Mono<Void> receive = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> processMessage(payload, tradingPair))
                .then();

        return Mono.when(send, receive)
                .then(Mono.error(new RuntimeException("NSE WebSocket session closed - reconnecting")));
    }

    /**
     * Processes a single WebSocket message: captures T0, delegates to {@link NseMessageParser},
     * and emits the result through the sink.
     *
     * <p><b>LATENCY CRITICAL:</b> {@code System.nanoTime()} MUST be the very first operation.
     * This is T0 — the ground truth for pipeline latency. No code may precede it.</p>
     *
     * <p>Heartbeat pongs ({@code "pong"}) update {@link #lastMessageNanoTime} (proving the feed
     * is alive) but are not forwarded to the parser — they are not market data.</p>
     *
     * @param payload     the raw string from the WebSocket
     * @param tradingPair the initial trigger pair (parser resolves pair from token in 7A.2)
     */
    void processMessage(String payload, TradingPair tradingPair) {
        // ============================================================
        // T0 — GROUND TRUTH TIMESTAMP
        // This MUST be the very first line. Do not add anything above.
        // ============================================================
        final long receivedNanos = System.nanoTime();

        lastMessageNanoTime.set(receivedNanos);

        if ("pong".equalsIgnoreCase(payload.trim())) {
            log.debug("NSE heartbeat pong received");
            return;
        }

        metrics.recordMessageReceived(ExchangeId.NSE);

        if (feedStatus.compareAndSet(FeedStatus.STALE, FeedStatus.CONNECTED)) {
            metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.CONNECTED);
        }

        Optional<NormalisedTick> maybeTick = messageParser.parse(payload, tradingPair, receivedNanos);

        maybeTick.ifPresent(tick -> {
            Sinks.EmitResult result = sink.tryEmitNext(tick);
            if (result.isFailure()) {
                log.warn("Failed to emit NSE tick: result={}", result);
            }
        });
    }

    /**
     * Starts a periodic staleness checker that monitors message freshness.
     *
     * <p>NSE uses a 60-second staleness threshold (vs 500 ms for crypto) to accommodate
     * lower tick frequency and legitimate quiet periods (pre-open auction, circuit breakers,
     * halts). Outside market hours (15:30–09:15 IST) no ticks arrive at all — the connector
     * stays CONNECTED so the health dashboard can show staleness age rather than triggering
     * pointless reconnect attempts.</p>
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
                            metrics.recordFeedStatus(ExchangeId.NSE, FeedStatus.STALE);
                            log.warn("NSE feed went STALE: noMessageFor={}ms threshold={}ms",
                                    elapsedNanos / 1_000_000, threshold.toMillis());
                        }
                    }
                });
    }
}
