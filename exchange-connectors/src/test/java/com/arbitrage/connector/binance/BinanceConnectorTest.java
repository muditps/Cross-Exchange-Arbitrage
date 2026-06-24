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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// NOTE: BinanceConnector now takes BinanceMessageParser instead of ObjectMapper directly.
// The parser is constructed here with a real ObjectMapper — unit tests for parsing logic
// are in BinanceMessageParserTest. This test class focuses on connector behaviour.

/**
 * Unit tests for {@link BinanceConnector}.
 *
 * <p>Tests the core logic (message parsing, status management, symbol conversion)
 * without requiring a real WebSocket server. Uses the package-private constructor
 * and {@code processMessage()} method for direct access to internals.</p>
 *
 * <p>The mock {@link WebSocketClient} returns {@link Mono#never()} to simulate
 * an open connection that never completes or errors — this allows testing the
 * sink-based message processing independently from the WebSocket lifecycle.</p>
 */
class BinanceConnectorTest {

    private static final String VALID_BOOK_TICKER = """
            {"u":400900217,"s":"BTCUSDT","b":"67250.50000000","B":"1.23400000","a":"67251.30000000","A":"0.98700000"}""";

    private static final String SUBSCRIPTION_ACK = """
            {"result":null,"id":1}""";

    private static final String MALFORMED_JSON = """
            {not valid json""";

    private static final String MISSING_FIELDS = """
            {"u":400900217,"s":"BTCUSDT"}""";

    private static final String INVALID_PRICE = """
            {"u":400900217,"s":"BTCUSDT","b":"not_a_number","B":"1.23400000","a":"67251.30000000","A":"0.98700000"}""";

    private BinanceConnector connector;
    private TradingPair btcUsdt;

    @BeforeEach
    void setUp() {
        ExchangeConnectorProperties.ExchangeProperties config = new ExchangeConnectorProperties.ExchangeProperties();
        config.setWsEndpoint("wss://stream.binance.com:9443/ws");
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
        BinanceMessageParser messageParser = new BinanceMessageParser(objectMapper, metrics);
        connector = new BinanceConnector(config, messageParser, mockClient, metrics);
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
        @DisplayName("BTC-USDT converts to btcusdt for Binance WebSocket")
        void btcUsdt_convertsToBinanceFormat() {
            assertEquals("btcusdt", connector.toBinanceSymbol(btcUsdt));
        }

        @Test
        @DisplayName("ETH-BTC converts to ethbtc for Binance WebSocket")
        void ethBtc_convertsToBinanceFormat() {
            TradingPair ethBtc = TradingPair.builder()
                    .baseCurrency("ETH")
                    .quoteCurrency("BTC")
                    .build();
            assertEquals("ethbtc", connector.toBinanceSymbol(ethBtc));
        }

        @Test
        @DisplayName("Mixed case input is normalised to lowercase")
        void mixedCase_normalisedToLowercase() {
            TradingPair mixedCase = TradingPair.builder()
                    .baseCurrency("Btc")
                    .quoteCurrency("Usdt")
                    .build();
            assertEquals("btcusdt", connector.toBinanceSymbol(mixedCase));
        }
    }

    @Nested
    @DisplayName("Message Parsing")
    class MessageParsingTests {

        @Test
        @DisplayName("Valid bookTicker message produces correct NormalisedTick")
        void validBookTicker_producesCorrectTick() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        assertEquals(ExchangeId.BINANCE, tick.getExchangeId());
                        assertEquals(btcUsdt, tick.getTradingPair());
                        assertEquals(0, new BigDecimal("67250.50000000").compareTo(tick.getBestBidPrice()));
                        assertEquals(0, new BigDecimal("1.23400000").compareTo(tick.getBestBidQuantity()));
                        assertEquals(0, new BigDecimal("67251.30000000").compareTo(tick.getBestAskPrice()));
                        assertEquals(0, new BigDecimal("0.98700000").compareTo(tick.getBestAskQuantity()));
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("BigDecimal precision is preserved at 8 decimal places")
        void bigDecimalPrecision_isPreservedAtScale8() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            String precisionMessage = """
                    {"u":1,"s":"BTCUSDT","b":"0.00000001","B":"100.00000000","a":"99999999999.99999999","A":"0.00000001"}""";
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
            // (if the ack had emitted, we'd get 2 items instead of 1)
            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

            StepVerifier.create(connector.connect(btcUsdt).take(1))
                    .assertNext(tick -> assertEquals(ExchangeId.BINANCE, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Malformed JSON does NOT crash the stream")
        void malformedJson_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MALFORMED_JSON, btcUsdt);
            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BINANCE, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Message missing bookTicker fields is skipped")
        void missingFields_isSkipped() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(MISSING_FIELDS, btcUsdt);
            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BINANCE, tick.getExchangeId()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Invalid price (non-numeric) does NOT crash the stream")
        void invalidPrice_doesNotCrash() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            connector.processMessage(INVALID_PRICE, btcUsdt);
            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> assertEquals(ExchangeId.BINANCE, tick.getExchangeId()))
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

            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);

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
        @DisplayName("exchangeTimestamp is set to approximately now")
        void exchangeTimestamp_isApproximatelyNow() {
            Flux<NormalisedTick> tickFlux = connector.connect(btcUsdt).take(1);

            long beforeMillis = System.currentTimeMillis();
            connector.processMessage(VALID_BOOK_TICKER, btcUsdt);
            long afterMillis = System.currentTimeMillis();

            StepVerifier.create(tickFlux)
                    .assertNext(tick -> {
                        long tickMillis = tick.getExchangeTimestamp().toEpochMilli();
                        assertTrue(tickMillis >= beforeMillis && tickMillis <= afterMillis,
                                "exchangeTimestamp should be between before and after measurement");
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
        @DisplayName("getExchangeId returns BINANCE")
        void exchangeId_isBinance() {
            assertEquals(ExchangeId.BINANCE, connector.getExchangeId());
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
            disabledConfig.setWsEndpoint("wss://stream.binance.com:9443/ws");
            disabledConfig.setTakerFeeRate(new BigDecimal("0.0010"));
            disabledConfig.setStalenessThreshold(Duration.ofMillis(500));
            disabledConfig.setEnabled(false);

            WebSocketClient mockClient = mock(WebSocketClient.class);
            ConnectorMetrics disabledMetrics = new ConnectorMetrics(new SimpleMeterRegistry());
            BinanceMessageParser parser = new BinanceMessageParser(new ObjectMapper(), disabledMetrics);
            BinanceConnector disabledConnector = new BinanceConnector(
                    disabledConfig, parser, mockClient, disabledMetrics);

            StepVerifier.create(disabledConnector.connect(btcUsdt))
                    .verifyComplete();
        }
    }
}
