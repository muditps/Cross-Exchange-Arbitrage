package com.arbitrage.connector.kucoin;

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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KuCoinConnector}.
 *
 * <p>Uses mock dependencies to test message processing, feed status management,
 * and the REST token bootstrap flow without real network calls.</p>
 *
 * <p>The mock {@link WebSocketClient} returns {@link Mono#never()} to simulate
 * an open connection that never completes. The mock {@link KuCoinTokenService}
 * returns a pre-configured {@link KuCoinConnectionDetails} synchronously.
 * This lets us test sink-based message processing in isolation.</p>
 *
 * <p><b>Key difference from BinanceConnectorTest and BybitConnectorTest:</b>
 * KuCoin-specific message types (welcome, ack) and the canonical symbol
 * passthrough (no conversion needed since KuCoin uses BTC-USDT format).</p>
 */
class KuCoinConnectorTest {

    private static final String VALID_TICKER = """
            {"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker",\
            "data":{"bestBid":"21109.50000000","bestBidSize":"0.50000000",\
            "bestAsk":"21109.60000000","bestAskSize":"0.30000000","time":1673853746003}}""";

    private static final String SECOND_TICKER = """
            {"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker",\
            "data":{"bestBid":"21115.20000000","bestBidSize":"1.75000000",\
            "bestAsk":"21115.80000000","bestAskSize":"0.92000000","time":1673853747150}}""";

    private static final String WELCOME_MESSAGE = """
            {"id":"abc123","type":"welcome"}""";

    private static final String SUBSCRIPTION_ACK = """
            {"id":"sub-1","type":"ack"}""";

    private static final String PONG_MESSAGE = """
            {"id":"ping-0","type":"pong"}""";

    private static final String MALFORMED_JSON = """
            {not valid json""";

    private static final String MISSING_DATA_FIELDS = """
            {"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker",\
            "data":{"bestBid":"21109.50"}}""";

    private static final String INVALID_PRICE = """
            {"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker",\
            "data":{"bestBid":"not_a_number","bestBidSize":"0.5",\
            "bestAsk":"21109.60","bestAskSize":"0.3","time":1673853746003}}""";

    private KuCoinConnector connector;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        ExchangeConnectorProperties.ExchangeProperties config = buildConfig();

        // Mock WebSocketClient returns Mono.never() — simulates open connection
        WebSocketClient mockWsClient = mock(WebSocketClient.class);
        when(mockWsClient.execute(any(URI.class), any())).thenReturn(Mono.never());

        // Mock KuCoinTokenService returns connection details immediately (no HTTP needed)
        KuCoinConnectionDetails details = new KuCoinConnectionDetails(
                "test-token", "wss://ws-api.kucoin.com/endpoint", 18000L);
        KuCoinTokenService mockTokenService = new KuCoinTokenService(WebClient.create(), "http://localhost/unused") {
            @Override
            public synchronized Mono<KuCoinConnectionDetails> getConnectionDetails() {
                return Mono.just(details);
            }
        };

        ObjectMapper objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        KuCoinMessageParser messageParser = new KuCoinMessageParser(objectMapper, metrics);
        connector = new KuCoinConnector(config, mockTokenService, messageParser, mockWsClient, metrics);
        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    @AfterEach
    void tearDown() {
        connector.disconnect();
    }

    // ============================================================
    // Message Parsing Tests
    // ============================================================

    @Nested
    @DisplayName("Message Parsing")
    class MessageParsingTests {

        @Test
        @DisplayName("Valid ticker message produces correct NormalisedTick")
        void validTicker_producesCorrectTick() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertEquals(ExchangeId.KUCOIN, tick.getExchangeId());
                        assertEquals(btcUsdt, tick.getTradingPair());
                        assertEquals(0, new BigDecimal("21109.50000000").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("0.50000000").compareTo(tick.getBestBidQuantity()));
                        assertEquals(0, new BigDecimal("21109.60000000").compareTo(tick.getBestAskPrice()));
                        assertEquals(0, new BigDecimal("0.30000000").compareTo(tick.getBestAskQuantity()));
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("Welcome message does NOT produce a tick")
        void welcomeMessage_doesNotProduceTick() {
            connector.connect(btcUsdt);
            connector.processMessage(WELCOME_MESSAGE, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Subscription ack does NOT produce a tick")
        void subscriptionAck_doesNotProduceTick() {
            connector.connect(btcUsdt);
            connector.processMessage(SUBSCRIPTION_ACK, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Pong message does NOT produce a tick")
        void pongMessage_doesNotProduceTick() {
            connector.connect(btcUsdt);
            connector.processMessage(PONG_MESSAGE, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Malformed JSON does NOT crash the stream")
        void malformedJson_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MALFORMED_JSON, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Incomplete data fields are skipped")
        void missingFields_isSkipped() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MISSING_DATA_FIELDS, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Invalid price does NOT crash the stream")
        void invalidPrice_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(INVALID_PRICE, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.KUCOIN, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("BigDecimal precision is preserved")
        void bigDecimalPrecision_isPreserved() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);
            String precisionMessage = "{\"type\":\"message\",\"topic\":\"/market/ticker:BTC-USDT\","
                    + "\"subject\":\"trade.ticker\",\"data\":{\"bestBid\":\"0.00000001\","
                    + "\"bestBidSize\":\"100.00000000\",\"bestAsk\":\"99999999999.99999999\","
                    + "\"bestAskSize\":\"0.00000001\",\"time\":1673853746003}}";

            connector.processMessage(precisionMessage, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertEquals(0, new BigDecimal("0.00000001").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("99999999999.99999999").compareTo(tick.getBestAskPrice()));
                    })
                    .verifyComplete();
        }
    }

    // ============================================================
    // Timestamp Tests
    // ============================================================

    @Nested
    @DisplayName("Timestamp Handling")
    class TimestampTests {

        @Test
        @DisplayName("T0 is captured before T1")
        void t0_isCapturedBeforeT1() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertTrue(tick.getReceivedTimestamp() > 0, "T0 must be set");
                        assertTrue(tick.getProcessedTimestamp() > 0, "T1 must be set");
                        assertTrue(tick.getReceivedTimestamp() < tick.getProcessedTimestamp(),
                                "T0 (received) must be before T1 (processed)");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("exchangeTimestamp matches data.time field (not Instant.now())")
        void exchangeTimestamp_matchesDataTimeField() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        // VALID_TICKER has time=1673853746003
                        Instant expectedTimestamp = Instant.ofEpochMilli(1673853746003L);
                        assertEquals(expectedTimestamp, tick.getExchangeTimestamp(),
                                "exchangeTimestamp must match data.time, not Instant.now()");
                    })
                    .verifyComplete();
        }
    }

    // ============================================================
    // Feed Status Tests
    // ============================================================

    @Nested
    @DisplayName("Feed Status Management")
    class FeedStatusTests {

        @Test
        @DisplayName("Initial feed status is DISCONNECTED")
        void initialStatus_isDisconnected() {
            assertEquals(FeedStatus.DISCONNECTED, connector.getFeedStatus());
        }

        @Test
        @DisplayName("getExchangeId() returns KUCOIN")
        void exchangeId_isKucoin() {
            assertEquals(ExchangeId.KUCOIN, connector.getExchangeId());
        }

        @Test
        @DisplayName("disconnect() is idempotent")
        void disconnect_isIdempotent() {
            connector.disconnect();
            connector.disconnect();
            assertEquals(FeedStatus.DISCONNECTED, connector.getFeedStatus());
        }

        @Test
        @DisplayName("disconnect() after connect() returns to DISCONNECTED")
        void disconnectAfterConnect_returnsToDisconnected() {
            connector.connect(btcUsdt);
            connector.disconnect();
            assertEquals(FeedStatus.DISCONNECTED, connector.getFeedStatus());
        }
    }

    // ============================================================
    // Disabled Connector Tests
    // ============================================================

    @Nested
    @DisplayName("Disabled Connector")
    class DisabledConnectorTests {

        @Test
        @DisplayName("Disabled connector returns empty Flux")
        void disabledConnector_returnsEmptyFlux() {
            ExchangeConnectorProperties.ExchangeProperties disabledConfig = buildConfig();
            disabledConfig.setEnabled(false);

            WebSocketClient mockWsClient = mock(WebSocketClient.class);
            KuCoinConnectionDetails details = new KuCoinConnectionDetails(
                    "test-token", "wss://ws-api.kucoin.com/endpoint", 18000L);
            KuCoinTokenService mockTokenService = new KuCoinTokenService(WebClient.create(), "http://localhost/unused") {
                @Override
                public synchronized Mono<KuCoinConnectionDetails> getConnectionDetails() {
                    return Mono.just(details);
                }
            };

            ConnectorMetrics disabledMetrics = new ConnectorMetrics(new SimpleMeterRegistry());
            KuCoinMessageParser parser = new KuCoinMessageParser(new ObjectMapper(), disabledMetrics);
            KuCoinConnector disabledConnector = new KuCoinConnector(
                    disabledConfig, mockTokenService, parser, mockWsClient, disabledMetrics);

            StepVerifier.create(disabledConnector.connect(btcUsdt))
                    .verifyComplete();
        }
    }

    // ============================================================
    // Helpers
    // ============================================================

    private static ExchangeConnectorProperties.ExchangeProperties buildConfig() {
        ExchangeConnectorProperties.ExchangeProperties config =
                new ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint("https://api.kucoin.com/api/v1/bullet-public");
        config.setTakerFeeRate(new BigDecimal("0.0010"));
        config.setInitialReconnectDelay(Duration.ofSeconds(1));
        config.setMaxReconnectDelay(Duration.ofSeconds(30));
        config.setMaxReconnectAttempts(10);
        config.setStalenessThreshold(Duration.ofMillis(500));
        config.setEnabled(true);
        return config;
    }
}
