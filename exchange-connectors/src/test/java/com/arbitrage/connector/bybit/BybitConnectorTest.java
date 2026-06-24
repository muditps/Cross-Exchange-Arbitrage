package com.arbitrage.connector.bybit;

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
 * Unit tests for {@link BybitConnector}.
 *
 * <p>Tests the core logic (message parsing, status management, symbol conversion,
 * heartbeat awareness) without requiring a real WebSocket server. Uses the
 * package-private constructor and {@code processMessage()} method for direct
 * access to internals.</p>
 *
 * <p>The mock {@link WebSocketClient} returns {@link Mono#never()} to simulate
 * an open connection that never completes or errors — this allows testing the
 * sink-based message processing independently from the WebSocket lifecycle.</p>
 *
 * <p><b>Key difference from BinanceConnectorTest:</b> Bybit-specific message
 * formats (nested data object, pong responses, delta message type) and the
 * exchange timestamp verification (server {@code ts} vs {@code Instant.now()}).</p>
 */
class BybitConnectorTest {

    private static final String VALID_TICKER = """
            {"topic":"tickers.BTCUSDT","ts":1673853746003,"type":"snapshot","cs":2588407389,\
            "data":{"symbol":"BTCUSDT","bid1Price":"21109.50000000","bid1Size":"0.50000000",\
            "ask1Price":"21109.60000000","ask1Size":"0.30000000"}}""";

    private static final String DELTA_TICKER = """
            {"topic":"tickers.BTCUSDT","ts":1673853747150,"type":"delta","cs":2588407412,\
            "data":{"symbol":"BTCUSDT","bid1Price":"21115.20000000","bid1Size":"1.75000000",\
            "ask1Price":"21115.80000000","ask1Size":"0.92000000"}}""";

    private static final String SUBSCRIPTION_ACK = """
            {"success":true,"ret_msg":"subscribe","conn_id":"abc123","req_id":"1","op":"subscribe"}""";

    private static final String PONG_MESSAGE = """
            {"success":true,"ret_msg":"pong","conn_id":"abc123","req_id":"heartbeat","op":"pong"}""";

    private static final String MALFORMED_JSON = """
            {not valid json""";

    private static final String MISSING_DATA_FIELDS = """
            {"topic":"tickers.BTCUSDT","ts":1673853746003,"type":"snapshot","cs":1,\
            "data":{"symbol":"BTCUSDT"}}""";

    private static final String INVALID_PRICE = """
            {"topic":"tickers.BTCUSDT","ts":1673853746003,"type":"snapshot","cs":1,\
            "data":{"symbol":"BTCUSDT","bid1Price":"not_a_number","bid1Size":"0.5",\
            "ask1Price":"21109.60","ask1Size":"0.3"}}""";

    private BybitConnector connector;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        ExchangeConnectorProperties.ExchangeProperties config = new ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint("wss://stream.bybit.com/v5/public/spot");
        config.setTakerFeeRate(new BigDecimal("0.0010"));
        config.setInitialReconnectDelay(Duration.ofSeconds(1));
        config.setMaxReconnectDelay(Duration.ofSeconds(30));
        config.setMaxReconnectAttempts(10);
        config.setStalenessThreshold(Duration.ofMillis(500));
        config.setEnabled(true);

        // Mock WebSocketClient returns Mono.never() — simulates an open connection
        // that never completes. This lets us test sink-based message processing
        // without the WebSocket lifecycle interfering.
        WebSocketClient mockClient = mock(WebSocketClient.class);
        when(mockClient.execute(any(URI.class), any())).thenReturn(Mono.never());

        ObjectMapper objectMapper = new ObjectMapper();
        ConnectorMetrics metrics = new ConnectorMetrics(new SimpleMeterRegistry());
        BybitMessageParser messageParser = new BybitMessageParser(objectMapper, metrics);
        connector = new BybitConnector(config, messageParser, mockClient, metrics);
        btcUsdt = TradingPair.builder()
                .baseCurrency("BTC")
                .quoteCurrency("USDT")
                .build();
    }

    @AfterEach
    void tearDown() {
        connector.disconnect();
    }

    @Nested
    @DisplayName("Symbol Conversion")
    class SymbolConversionTests {

        @Test
        @DisplayName("BTC-USDT converts to BTCUSDT for Bybit WebSocket")
        void btcUsdt_convertsToBybitFormat() {
            assertEquals("BTCUSDT", connector.toBybitSymbol(btcUsdt));
        }

        @Test
        @DisplayName("ETH-BTC converts to ETHBTC for Bybit WebSocket")
        void ethBtc_convertsToBybitFormat() {
            TradingPair ethBtc = TradingPair.builder()
                    .baseCurrency("ETH")
                    .quoteCurrency("BTC")
                    .build();
            assertEquals("ETHBTC", connector.toBybitSymbol(ethBtc));
        }

        @Test
        @DisplayName("Mixed case input is normalised to uppercase")
        void mixedCase_normalisedToUppercase() {
            TradingPair mixedCase = TradingPair.builder()
                    .baseCurrency("Btc")
                    .quoteCurrency("Usdt")
                    .build();
            assertEquals("BTCUSDT", connector.toBybitSymbol(mixedCase));
        }
    }

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
                        assertEquals(ExchangeId.BYBIT, tick.getExchangeId());
                        assertEquals(btcUsdt, tick.getTradingPair());
                        assertEquals(0, new BigDecimal("21109.50000000").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("0.50000000").compareTo(tick.getBestBidQuantity()));
                        assertEquals(0, new BigDecimal("21109.60000000").compareTo(tick.getBestAskPrice()));
                        assertEquals(0, new BigDecimal("0.30000000").compareTo(tick.getBestAskQuantity()));
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("BigDecimal precision is preserved at 8 decimal places")
        void bigDecimalPrecision_isPreservedAtScale8() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            String precisionMessage = "{\"topic\":\"tickers.BTCUSDT\",\"ts\":1673853746003,"
                    + "\"type\":\"snapshot\",\"cs\":1,"
                    + "\"data\":{\"symbol\":\"BTCUSDT\",\"bid1Price\":\"0.00000001\","
                    + "\"bid1Size\":\"100.00000000\",\"ask1Price\":\"99999999999.99999999\","
                    + "\"ask1Size\":\"0.00000001\"}}";
            connector.processMessage(precisionMessage, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertEquals(0, new BigDecimal("0.00000001").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("99999999999.99999999").compareTo(tick.getBestAskPrice()));
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("Subscription ack message does NOT produce a tick")
        void subscriptionAck_doesNotEmitTick() {
            connector.connect(btcUsdt);

            connector.processMessage(SUBSCRIPTION_ACK, btcUsdt);

            // Send a valid message after the ack to prove the ack was skipped
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.BYBIT, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Pong message does NOT produce a tick")
        void pongMessage_doesNotEmitTick() {
            connector.connect(btcUsdt);

            connector.processMessage(PONG_MESSAGE, btcUsdt);

            // Send a valid message after the pong to prove the pong was skipped
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.BYBIT, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Malformed JSON does NOT crash the stream")
        void malformedJson_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MALFORMED_JSON, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BYBIT, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Message missing data fields is skipped")
        void missingFields_isSkipped() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MISSING_DATA_FIELDS, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BYBIT, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Invalid price (non-numeric) does NOT crash the stream")
        void invalidPrice_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(INVALID_PRICE, btcUsdt);
            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BYBIT, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Delta message type produces correct tick (same as snapshot)")
        void deltaMessage_producesCorrectTick() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(DELTA_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertEquals(ExchangeId.BYBIT, tick.getExchangeId());
                        assertEquals(0, new BigDecimal("21115.20000000").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("21115.80000000").compareTo(tick.getBestAskPrice()));
                    })
                    .verifyComplete();
        }
    }

    @Nested
    @DisplayName("T0 Timestamp Capture")
    class TimestampTests {

        @Test
        @DisplayName("receivedTimestamp (T0) is captured before processedTimestamp (T1)")
        void receivedTimestamp_isBeforeProcessedTimestamp() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertTrue(tick.getReceivedTimestamp() > 0,
                                "receivedTimestamp must be set");
                        assertTrue(tick.getProcessedTimestamp() > 0,
                                "processedTimestamp must be set");
                        assertTrue(tick.getReceivedTimestamp() < tick.getProcessedTimestamp(),
                                "T0 (received) must be before T1 (processed)");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("exchangeTimestamp matches server ts field (not Instant.now())")
        void exchangeTimestamp_matchesServerTs() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        // The VALID_TICKER has ts=1673853746003
                        Instant expectedTimestamp = Instant.ofEpochMilli(1673853746003L);
                        assertEquals(expectedTimestamp, tick.getExchangeTimestamp(),
                                "exchangeTimestamp must match server ts field, not Instant.now()");
                    })
                    .verifyComplete();
        }
    }

    @Nested
    @DisplayName("Feed Status Management")
    class FeedStatusTests {

        @Test
        @DisplayName("Initial feed status is DISCONNECTED")
        void initialStatus_isDisconnected() {
            assertEquals(FeedStatus.DISCONNECTED, connector.getFeedStatus());
        }

        @Test
        @DisplayName("getExchangeId returns BYBIT")
        void exchangeId_isBybit() {
            assertEquals(ExchangeId.BYBIT, connector.getExchangeId());
        }

        @Test
        @DisplayName("disconnect() is idempotent — calling twice does not throw")
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

    @Nested
    @DisplayName("Disabled Connector")
    class DisabledConnectorTests {

        @Test
        @DisplayName("Disabled connector returns empty Flux")
        void disabledConnector_returnsEmptyFlux() {
            ExchangeConnectorProperties.ExchangeProperties disabledConfig =
                    new ExchangeConnectorProperties.ExchangeProperties();
            disabledConfig.setWsEndpoint("wss://stream.bybit.com/v5/public/spot");
            disabledConfig.setTakerFeeRate(new BigDecimal("0.0010"));
            disabledConfig.setStalenessThreshold(Duration.ofMillis(500));
            disabledConfig.setEnabled(false);

            WebSocketClient mockClient = mock(WebSocketClient.class);
            ConnectorMetrics disabledMetrics = new ConnectorMetrics(new SimpleMeterRegistry());
            BybitMessageParser parser = new BybitMessageParser(new ObjectMapper(), disabledMetrics);
            BybitConnector disabledConnector = new BybitConnector(
                    disabledConfig, parser, mockClient, disabledMetrics);

            StepVerifier.create(disabledConnector.connect(btcUsdt))
                    .verifyComplete();
        }
    }
}
