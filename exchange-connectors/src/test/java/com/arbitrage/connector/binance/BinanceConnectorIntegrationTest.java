package com.arbitrage.connector.binance;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnectorProperties;
import com.arbitrage.connector.metrics.ConnectorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link BinanceConnector} using a local mock WebSocket server.
 *
 * <p>Uses Reactor Netty's {@link HttpServer} to create a lightweight WebSocket endpoint
 * on a random port. The server sends a subscription acknowledgment followed by bookTicker
 * messages, simulating Binance's real behavior. The test verifies the full end-to-end flow:
 * connect → subscribe → receive → parse → emit NormalisedTick.</p>
 *
 * <p>This is NOT a Testcontainers test — it runs entirely in-process without Docker.
 * The formal Testcontainers integration test with Kafka is created in Session 1.7.</p>
 */
class BinanceConnectorIntegrationTest {

    private static final String SUBSCRIPTION_ACK = "{\"result\":null,\"id\":1}";
    private static final String BOOK_TICKER_1 =
            "{\"u\":400900217,\"s\":\"BTCUSDT\",\"b\":\"67250.50000000\",\"B\":\"1.23400000\",\"a\":\"67251.30000000\",\"A\":\"0.98700000\"}";
    private static final String BOOK_TICKER_2 =
            "{\"u\":400900218,\"s\":\"BTCUSDT\",\"b\":\"67255.10000000\",\"B\":\"2.50000000\",\"a\":\"67256.00000000\",\"A\":\"1.10000000\"}";

    private DisposableServer mockServer;
    private BinanceConnector connector;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        // Start a local WebSocket server that simulates Binance:
        // 1. Receives the subscription message (ignored in this simple mock)
        // 2. Sends back a subscription ack
        // 3. Sends two bookTicker messages
        mockServer = HttpServer.create()
                .port(0)  // random available port
                .route(routes -> routes.ws("/ws", (wsInbound, wsOutbound) -> {
                    // Read and ignore the incoming subscription message, then send test data
                    Mono<Void> receive = wsInbound.receive().then();

                    Flux<String> responses = Flux.just(SUBSCRIPTION_ACK, BOOK_TICKER_1, BOOK_TICKER_2)
                            .delayElements(Duration.ofMillis(50));  // small delay between messages

                    Mono<Void> send = wsOutbound.sendString(responses).then();

                    return Mono.when(send, receive);
                }))
                .bindNow();

        int port = mockServer.port();

        ExchangeConnectorProperties.ExchangeProperties config = new ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint("ws://localhost:" + port + "/ws");
        config.setTakerFeeRate(new BigDecimal("0.0010"));
        config.setInitialReconnectDelay(Duration.ofSeconds(1));
        config.setMaxReconnectDelay(Duration.ofSeconds(5));
        config.setMaxReconnectAttempts(3);
        config.setStalenessThreshold(Duration.ofSeconds(5));
        config.setEnabled(true);

        ObjectMapper objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        BinanceMessageParser messageParser = new BinanceMessageParser(objectMapper, metrics);
        connector = new BinanceConnector(config, messageParser, new ReactorNettyWebSocketClient(), metrics);
        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    @AfterEach
    void tearDown() {
        connector.disconnect();
        if (mockServer != null) {
            mockServer.disposeNow();
        }
    }

    @Test
    @DisplayName("End-to-end: connect to mock server, receive and parse bookTicker messages")
    void endToEnd_connectAndReceiveBookTickers() {
        Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(2);

        StepVerifier.create(tickFlux)
                .assertNext(tick -> {
                    assertEquals(ExchangeId.BINANCE, tick.getExchangeId());
                    assertEquals(btcUsdt, tick.getTradingPair());
                    assertEquals(0, new BigDecimal("67250.50000000").compareTo(tick.getBestBidPrice()));
                    assertEquals(0, new BigDecimal("67251.30000000").compareTo(tick.getBestAskPrice()));
                    assertEquals(0, new BigDecimal("1.23400000").compareTo(tick.getBestBidQuantity()));
                    assertEquals(0, new BigDecimal("0.98700000").compareTo(tick.getBestAskQuantity()));
                    assertTrue(tick.getReceivedTimestamp() > 0);
                    assertTrue(tick.getProcessedTimestamp() > tick.getReceivedTimestamp());
                })
                .assertNext(tick -> {
                    assertEquals(0, new BigDecimal("67255.10000000").compareTo(tick.getBestBidPrice()));
                    assertEquals(0, new BigDecimal("67256.00000000").compareTo(tick.getBestAskPrice()));
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Feed status transitions to CONNECTED after receiving messages")
    void feedStatus_transitionsToConnected() {
        connector.connect(btcUsdt);

        // Wait for the connection to establish and messages to arrive
        StepVerifier.create(
                        Flux.interval(Duration.ofMillis(50))
                                .map(i -> connector.getFeedStatus())
                                .filter(status -> status == FeedStatus.CONNECTED)
                                .take(1)
                )
                .assertNext(status -> assertEquals(FeedStatus.CONNECTED, status))
                .verifyComplete();
    }

    @Test
    @DisplayName("Subscription ack is filtered — only bookTicker messages produce ticks")
    void subscriptionAck_isFiltered() {
        // The mock server sends 3 messages: 1 ack + 2 bookTickers
        // We should only receive 2 ticks
        Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(2);

        StepVerifier.create(tickFlux)
                .expectNextCount(2)
                .verifyComplete();
    }
}
