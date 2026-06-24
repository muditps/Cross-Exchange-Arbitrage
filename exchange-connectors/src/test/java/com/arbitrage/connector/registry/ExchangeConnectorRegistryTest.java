package com.arbitrage.connector.registry;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.connector.ExchangeConnector;
import com.arbitrage.connector.TickKafkaProducer;
import com.arbitrage.connector.TradingPairsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.lenient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ExchangeConnectorRegistry}.
 *
 * <p>All exchange connectors and Kafka producers are mocked — no real WebSocket
 * connections or Kafka brokers are used. Tests verify lifecycle management,
 * status aggregation, and lookup behaviour.</p>
 *
 * <p><b>KEY CONCEPT — SmartLifecycle:</b> Spring calls {@link ExchangeConnectorRegistry#start()}
 * automatically after all beans are initialised (because {@link ExchangeConnectorRegistry#isAutoStartup()}
 * returns {@code true}). In tests, we call it manually to verify the correct delegation happens.</p>
 */
@ExtendWith(MockitoExtension.class)
class ExchangeConnectorRegistryTest {

    // Three mock producers — one per exchange
    @Mock private TickKafkaProducer mockBinanceProducer;
    @Mock private TickKafkaProducer mockBybitProducer;
    @Mock private TickKafkaProducer mockKuCoinProducer;

    // Three mock connectors — for status queries
    @Mock private ExchangeConnector mockBinanceConnector;
    @Mock private ExchangeConnector mockBybitConnector;
    @Mock private ExchangeConnector mockKuCoinConnector;

    private TradingPairsProperties tradingPairsProperties;
    private ExchangeConnectorRegistry registry;

    private static final TradingPair BTC_USDT = TradingPair.builder()
            .baseCurrency("BTC")
            .quoteCurrency("USDT")
            .build();

    private static final TradingPair ETH_USDT = TradingPair.builder()
            .baseCurrency("ETH")
            .quoteCurrency("USDT")
            .build();

    @BeforeEach
    void setUp() {
        // Wire exchange IDs to mock connectors and producers
        when(mockBinanceConnector.getExchangeId()).thenReturn(ExchangeId.BINANCE);
        when(mockBybitConnector.getExchangeId()).thenReturn(ExchangeId.BYBIT);
        when(mockKuCoinConnector.getExchangeId()).thenReturn(ExchangeId.KUCOIN);

        // lenient() because getExchangeId() is only invoked on producers inside start()/stop()/getProducer()
        // — tests that exercise isRunning(), isAutoStartup(), getConnector(), or stop()-before-start
        // do not trigger the log statements that call producer.getExchangeId(), so strict mode
        // would flag these as unnecessary stubs. The stubs are logically needed for registry
        // construction and runtime calls; lenient() suppresses the false-positive.
        lenient().when(mockBinanceProducer.getExchangeId()).thenReturn(ExchangeId.BINANCE);
        lenient().when(mockBybitProducer.getExchangeId()).thenReturn(ExchangeId.BYBIT);
        lenient().when(mockKuCoinProducer.getExchangeId()).thenReturn(ExchangeId.KUCOIN);

        tradingPairsProperties = new TradingPairsProperties();
        tradingPairsProperties.setPairs(List.of("BTC-USDT"));

        registry = new ExchangeConnectorRegistry(
                List.of(mockBinanceProducer, mockBybitProducer, mockKuCoinProducer),
                List.of(mockBinanceConnector, mockBybitConnector, mockKuCoinConnector),
                tradingPairsProperties
        );
    }

    @Nested
    @DisplayName("Lifecycle — start()")
    class StartTests {

        @Test
        @DisplayName("start() calls start(pair) on all producers for each configured pair")
        void start_callsStartOnAllProducersForEachPair() {
            registry.start();

            verify(mockBinanceProducer).start(BTC_USDT);
            verify(mockBybitProducer).start(BTC_USDT);
            verify(mockKuCoinProducer).start(BTC_USDT);
        }

        @Test
        @DisplayName("start() calls start(pair) for each configured pair when multiple pairs exist")
        void start_callsStartForEachPair() {
            tradingPairsProperties.setPairs(List.of("BTC-USDT", "ETH-USDT"));

            registry.start();

            // Each producer should be started for both pairs
            verify(mockBinanceProducer).start(BTC_USDT);
            verify(mockBinanceProducer).start(ETH_USDT);
            verify(mockBybitProducer).start(BTC_USDT);
            verify(mockBybitProducer).start(ETH_USDT);
            verify(mockKuCoinProducer).start(BTC_USDT);
            verify(mockKuCoinProducer).start(ETH_USDT);
        }

        @Test
        @DisplayName("start() sets isRunning to true")
        void start_setsRunningTrue() {
            assertFalse(registry.isRunning(), "Registry should not be running before start");

            registry.start();

            assertTrue(registry.isRunning(), "Registry should be running after start");
        }

        @Test
        @DisplayName("start() is idempotent — second call does not start producers again")
        void start_isIdempotent() {
            registry.start();
            registry.start();

            // Each producer should have been started exactly once per pair
            verify(mockBinanceProducer, times(1)).start(BTC_USDT);
            verify(mockBybitProducer, times(1)).start(BTC_USDT);
            verify(mockKuCoinProducer, times(1)).start(BTC_USDT);
        }
    }

    @Nested
    @DisplayName("Lifecycle — stop()")
    class StopTests {

        @Test
        @DisplayName("stop() calls stop() on all producers and disconnect() on all connectors")
        void stop_callsStopOnAllProducersAndDisconnectsConnectors() {
            registry.start();
            registry.stop();

            verify(mockBinanceProducer).stop();
            verify(mockBybitProducer).stop();
            verify(mockKuCoinProducer).stop();

            verify(mockBinanceConnector).disconnect();
            verify(mockBybitConnector).disconnect();
            verify(mockKuCoinConnector).disconnect();
        }

        @Test
        @DisplayName("stop() sets isRunning to false")
        void stop_setsRunningFalse() {
            registry.start();
            registry.stop();

            assertFalse(registry.isRunning(), "Registry should not be running after stop");
        }

        @Test
        @DisplayName("stop() before start() does not call stop on producers")
        void stop_beforeStart_doesNothing() {
            registry.stop();

            verify(mockBinanceProducer, never()).stop();
            verify(mockBybitProducer, never()).stop();
            verify(mockKuCoinProducer, never()).stop();
        }
    }

    @Nested
    @DisplayName("Status Reporting")
    class StatusTests {

        @Test
        @DisplayName("getStatuses() returns the current FeedStatus for each exchange")
        void getStatuses_returnsAllExchangeStatuses() {
            when(mockBinanceConnector.getFeedStatus()).thenReturn(FeedStatus.CONNECTED);
            when(mockBybitConnector.getFeedStatus()).thenReturn(FeedStatus.DISCONNECTED);
            when(mockKuCoinConnector.getFeedStatus()).thenReturn(FeedStatus.STALE);

            Map<ExchangeId, FeedStatus> statuses = registry.getStatuses();

            assertEquals(FeedStatus.CONNECTED, statuses.get(ExchangeId.BINANCE),
                    "Binance should be CONNECTED");
            assertEquals(FeedStatus.DISCONNECTED, statuses.get(ExchangeId.BYBIT),
                    "Bybit should be DISCONNECTED");
            assertEquals(FeedStatus.STALE, statuses.get(ExchangeId.KUCOIN),
                    "KuCoin should be STALE");
        }

        @Test
        @DisplayName("getStatuses() returns a status for all 3 exchanges")
        void getStatuses_coversAllExchanges() {
            when(mockBinanceConnector.getFeedStatus()).thenReturn(FeedStatus.DISCONNECTED);
            when(mockBybitConnector.getFeedStatus()).thenReturn(FeedStatus.DISCONNECTED);
            when(mockKuCoinConnector.getFeedStatus()).thenReturn(FeedStatus.DISCONNECTED);

            Map<ExchangeId, FeedStatus> statuses = registry.getStatuses();

            assertEquals(3, statuses.size(), "Should have a status for all 3 exchanges");
            assertTrue(statuses.containsKey(ExchangeId.BINANCE));
            assertTrue(statuses.containsKey(ExchangeId.BYBIT));
            assertTrue(statuses.containsKey(ExchangeId.KUCOIN));
        }
    }

    @Nested
    @DisplayName("Connector Lookup")
    class LookupTests {

        @Test
        @DisplayName("getConnector() returns the correct connector for a known exchange")
        void getConnector_returnsConnectorForKnownExchange() {
            Optional<ExchangeConnector> connector = registry.getConnector(ExchangeId.BINANCE);

            assertTrue(connector.isPresent(), "Should find the Binance connector");
            assertEquals(ExchangeId.BINANCE, connector.get().getExchangeId());
        }

        @Test
        @DisplayName("getConnector() returns empty for an exchange not registered")
        void getConnector_returnsEmptyForUnknownExchange() {
            // Create a registry with only Binance — Bybit is absent
            ExchangeConnectorRegistry singleConnectorRegistry = new ExchangeConnectorRegistry(
                    List.of(mockBinanceProducer),
                    List.of(mockBinanceConnector),
                    tradingPairsProperties
            );

            Optional<ExchangeConnector> connector = singleConnectorRegistry.getConnector(ExchangeId.BYBIT);

            assertTrue(connector.isEmpty(), "Should return empty for unregistered exchange");
        }

        @Test
        @DisplayName("getProducer() returns the correct producer for a known exchange")
        void getProducer_returnsProducerForKnownExchange() {
            Optional<TickKafkaProducer> producer = registry.getProducer(ExchangeId.KUCOIN);

            assertTrue(producer.isPresent(), "Should find the KuCoin producer");
            assertEquals(ExchangeId.KUCOIN, producer.get().getExchangeId());
        }

        @Test
        @DisplayName("getProducer() returns empty for an exchange not registered")
        void getProducer_returnsEmptyForUnknownExchange() {
            ExchangeConnectorRegistry singleProducerRegistry = new ExchangeConnectorRegistry(
                    List.of(mockBinanceProducer),
                    List.of(mockBinanceConnector),
                    tradingPairsProperties
            );

            Optional<TickKafkaProducer> producer = singleProducerRegistry.getProducer(ExchangeId.BYBIT);

            assertTrue(producer.isEmpty(), "Should return empty for unregistered exchange");
        }

        @Test
        @DisplayName("getProducers() returns all registered producers")
        void getProducers_returnsAllProducers() {
            List<TickKafkaProducer> producers = registry.getProducers();

            assertEquals(3, producers.size(), "Should return all 3 producers");
        }
    }

    @Nested
    @DisplayName("SmartLifecycle Contract")
    class SmartLifecycleTests {

        @Test
        @DisplayName("isAutoStartup() returns true — Spring starts registry automatically")
        void isAutoStartup_returnsTrue() {
            assertTrue(registry.isAutoStartup(),
                    "Registry must auto-start so Spring drives the lifecycle, not callers");
        }

        @Test
        @DisplayName("isRunning() returns false before start()")
        void isRunning_falseBeforeStart() {
            assertFalse(registry.isRunning());
        }

        @Test
        @DisplayName("isRunning() returns true after start() and false after stop()")
        void isRunning_tracksLifecycleCorrectly() {
            registry.start();
            assertTrue(registry.isRunning());

            registry.stop();
            assertFalse(registry.isRunning());
        }
    }
}
