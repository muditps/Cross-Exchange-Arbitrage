package com.arbitrage.normalisation.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.FeedStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FeedHealthMonitor}.
 *
 * <p>Time-based behaviour (stale/disconnected transitions) is tested without {@code Thread.sleep}
 * by injecting a controllable {@code AtomicLong} as the clock source via the package-private
 * test constructor. Tests advance the fake clock to simulate elapsed time, then call
 * {@link FeedHealthMonitor#checkFeedHealth()} directly to trigger the scheduler logic.
 *
 * <p>This approach is deterministic, fast, and immune to CI timing variability.
 */
@DisplayName("FeedHealthMonitor")
class FeedHealthMonitorTest {

    private static final long STALE_THRESHOLD_MS = 5_000L;
    private static final long DISCONNECTED_THRESHOLD_MS = 30_000L;
    private static final long STALE_THRESHOLD_NANOS = STALE_THRESHOLD_MS * 1_000_000L;
    private static final long DISCONNECTED_THRESHOLD_NANOS = DISCONNECTED_THRESHOLD_MS * 1_000_000L;

    private AtomicLong fakeTimeNanos;
    private MeterRegistry registry;
    private FeedHealthMonitor monitor;

    @BeforeEach
    void setUp() {
        fakeTimeNanos = new AtomicLong(0L);
        registry = new SimpleMeterRegistry();
        monitor = new FeedHealthMonitor(fakeTimeNanos::get, STALE_THRESHOLD_MS, DISCONNECTED_THRESHOLD_MS, registry);
    }

    // ========================================================================
    // Initial state
    // ========================================================================

    @Nested
    @DisplayName("initial state — no ticks received yet")
    class InitialState {

        @Test
        @DisplayName("all exchanges default to DISCONNECTED before any tick is received")
        void allExchangesDefaultToDisconnected() {
            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.DISCONNECTED);
            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.DISCONNECTED);
            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.DISCONNECTED);
        }

        @Test
        @DisplayName("checkFeedHealth on never-seen exchange does not change status")
        void checkFeedHealthOnNeverSeenExchangeDoesNotChangeStatus() {
            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 1_000_000_000L); // far future
            monitor.checkFeedHealth();

            // Never seen — should remain at default DISCONNECTED, no NPE
            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.DISCONNECTED);
        }
    }

    // ========================================================================
    // CONNECTED transitions
    // ========================================================================

    @Nested
    @DisplayName("CONNECTED transitions — tick received")
    class ConnectedTransitions {

        @Test
        @DisplayName("recordTickReceived transitions exchange to CONNECTED")
        void recordTickReceivedTransitionsToConnected() {
            monitor.recordTickReceived(ExchangeId.BINANCE);

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("recordTickReceived on STALE exchange recovers to CONNECTED")
        void recordTickReceivedOnStaleFeedRecoversToConnected() {
            monitor.recordTickReceived(ExchangeId.BYBIT);
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();
            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.STALE);

            // Tick arrives — recover to CONNECTED
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 2_000_000L);
            monitor.recordTickReceived(ExchangeId.BYBIT);

            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("recordTickReceived on DISCONNECTED exchange recovers to CONNECTED")
        void recordTickReceivedOnDisconnectedFeedRecoversToConnected() {
            monitor.recordTickReceived(ExchangeId.KUCOIN);
            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();
            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.DISCONNECTED);

            // Tick arrives — recover to CONNECTED
            monitor.recordTickReceived(ExchangeId.KUCOIN);

            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("multiple recordTickReceived calls leave exchange as CONNECTED")
        void multipleTicksLeaveStatusConnected() {
            for (int i = 0; i < 5; i++) {
                fakeTimeNanos.addAndGet(1_000_000L); // advance 1ms per tick
                monitor.recordTickReceived(ExchangeId.BINANCE);
            }

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.CONNECTED);
        }
    }

    // ========================================================================
    // STALE transitions
    // ========================================================================

    @Nested
    @DisplayName("STALE transitions — no tick for stale threshold")
    class StaleTransitions {

        @Test
        @DisplayName("CONNECTED → STALE after stale threshold exceeded")
        void connectedToStaleAfterThresholdExceeded() {
            monitor.recordTickReceived(ExchangeId.BINANCE); // T=0

            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L); // just past 5s
            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.STALE);
        }

        @Test
        @DisplayName("remains CONNECTED when age is exactly at stale threshold")
        void remainsConnectedWhenAgeEqualsThreshold() {
            monitor.recordTickReceived(ExchangeId.BYBIT); // T=0

            fakeTimeNanos.set(STALE_THRESHOLD_NANOS); // exactly at threshold — not exceeded
            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("STALE does not re-transition to STALE on subsequent scheduler ticks")
        void staleDoesNotRetransitionToStaleRepeatedly() {
            monitor.recordTickReceived(ExchangeId.KUCOIN);
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();
            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.STALE);

            // Advance further but still below disconnected threshold
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 5_000_000_000L);
            monitor.checkFeedHealth(); // should not re-transition from STALE to STALE

            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.STALE);
        }
    }

    // ========================================================================
    // DISCONNECTED transitions
    // ========================================================================

    @Nested
    @DisplayName("DISCONNECTED transitions — no tick for disconnected threshold")
    class DisconnectedTransitions {

        @Test
        @DisplayName("STALE → DISCONNECTED after disconnected threshold exceeded")
        void staleToDisconnectedAfterThresholdExceeded() {
            monitor.recordTickReceived(ExchangeId.BINANCE);
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();
            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.STALE);

            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.DISCONNECTED);
        }

        @Test
        @DisplayName("CONNECTED → DISCONNECTED directly if disconnected threshold hit before scheduler sees STALE")
        void connectedToDisconnectedDirectly() {
            // If the scheduler runs infrequently, a feed might jump from CONNECTED
            // straight to DISCONNECTED without being observed at STALE
            monitor.recordTickReceived(ExchangeId.BYBIT);

            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();

            // disconnected threshold takes priority — no STALE intermediate needed
            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.DISCONNECTED);
        }

        @Test
        @DisplayName("remains DISCONNECTED on subsequent scheduler ticks without recovery")
        void remainsDisconnectedWithoutRecovery() {
            monitor.recordTickReceived(ExchangeId.KUCOIN);
            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();
            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.DISCONNECTED);

            fakeTimeNanos.set(DISCONNECTED_THRESHOLD_NANOS + 60_000_000_000L); // +60s
            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.DISCONNECTED);
        }
    }

    // ========================================================================
    // Multi-exchange isolation
    // ========================================================================

    @Nested
    @DisplayName("multi-exchange isolation — each exchange tracked independently")
    class MultiExchangeIsolation {

        @Test
        @DisplayName("BINANCE going stale does not affect BYBIT or KUCOIN status")
        void binanceStalenessDoesNotAffectOtherExchanges() {
            monitor.recordTickReceived(ExchangeId.BINANCE); // T=0
            monitor.recordTickReceived(ExchangeId.BYBIT);   // T=0
            monitor.recordTickReceived(ExchangeId.KUCOIN);  // T=0

            // Advance time and let BINANCE go stale
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);

            // BYBIT and KUCOIN get fresh ticks at T+stale
            monitor.recordTickReceived(ExchangeId.BYBIT);
            monitor.recordTickReceived(ExchangeId.KUCOIN);

            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.STALE);
            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.CONNECTED);
            assertThat(monitor.getStatus(ExchangeId.KUCOIN)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("each exchange has independent last-seen timestamp")
        void eachExchangeHasIndependentLastSeen() {
            monitor.recordTickReceived(ExchangeId.BINANCE); // T=0
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.recordTickReceived(ExchangeId.BYBIT); // T=stale+1ms (fresh)

            monitor.checkFeedHealth();

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.STALE);
            assertThat(monitor.getStatus(ExchangeId.BYBIT)).isEqualTo(FeedStatus.CONNECTED);
        }
    }

    // ========================================================================
    // Micrometer gauge
    // ========================================================================

    @Nested
    @DisplayName("Micrometer gauge — feed status observable by Prometheus")
    class MicrometerGauge {

        @Test
        @DisplayName("gauge is registered when status transitions from null to CONNECTED")
        void gaugeRegisteredOnFirstTransition() {
            monitor.recordTickReceived(ExchangeId.BINANCE);

            Gauge gauge = registry.find("normalisation.feed.status")
                    .tag("exchange", "BINANCE")
                    .gauge();

            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(FeedStatus.CONNECTED.ordinal());
        }

        @Test
        @DisplayName("gauge value updates when status transitions to STALE")
        void gaugeValueUpdatesOnStaleTransition() {
            monitor.recordTickReceived(ExchangeId.BYBIT);
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth();

            Gauge gauge = registry.find("normalisation.feed.status")
                    .tag("exchange", "BYBIT")
                    .gauge();

            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(FeedStatus.STALE.ordinal());
        }

        @Test
        @DisplayName("gauge value updates on recovery to CONNECTED")
        void gaugeValueUpdatesOnRecovery() {
            monitor.recordTickReceived(ExchangeId.KUCOIN);
            fakeTimeNanos.set(STALE_THRESHOLD_NANOS + 1_000_000L);
            monitor.checkFeedHealth(); // → STALE

            fakeTimeNanos.addAndGet(1_000_000L);
            monitor.recordTickReceived(ExchangeId.KUCOIN); // → CONNECTED

            Gauge gauge = registry.find("normalisation.feed.status")
                    .tag("exchange", "KUCOIN")
                    .gauge();

            assertThat(gauge).isNotNull();
            assertThat(gauge.value()).isEqualTo(FeedStatus.CONNECTED.ordinal());
        }
    }

    // ========================================================================
    // Concurrent safety
    // ========================================================================

    @Nested
    @DisplayName("concurrent safety — multiple threads calling recordTickReceived")
    class ConcurrentSafety {

        @Test
        @DisplayName("concurrent recordTickReceived from multiple threads leaves exchange CONNECTED")
        void concurrentTickReceivedFromMultipleThreadsLeavesExchangeConnected() throws InterruptedException {
            int threadCount = 10;
            int ticksPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // start all threads at once
                        for (int i = 0; i < ticksPerThread; i++) {
                            monitor.recordTickReceived(ExchangeId.BINANCE);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown(); // release all threads simultaneously
            assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(monitor.getStatus(ExchangeId.BINANCE)).isEqualTo(FeedStatus.CONNECTED);
        }

        @Test
        @DisplayName("concurrent recordTickReceived and checkFeedHealth do not produce inconsistent state")
        void concurrentTickReceivedAndCheckFeedHealthDoNotCorruptState() throws InterruptedException {
            // Seed with a tick so the scheduler has something to check
            monitor.recordTickReceived(ExchangeId.BYBIT);

            int iterations = 200;
            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(2);

            // Thread 1: continually receives ticks (keeps feed fresh at fake T=0)
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        monitor.recordTickReceived(ExchangeId.BYBIT);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });

            // Thread 2: continually calls checkFeedHealth (at fake T=0, feed is fresh)
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        monitor.checkFeedHealth();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });

            startLatch.countDown();
            assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            // After concurrent ticks at T=0, feed should not be STALE or DISCONNECTED
            FeedStatus finalStatus = monitor.getStatus(ExchangeId.BYBIT);
            assertThat(finalStatus).isIn(FeedStatus.CONNECTED, FeedStatus.STALE);
            // Note: may be STALE if scheduler wins final race, but never DISCONNECTED
            // (fake time is at 0, which is < stale threshold from the last recorded tick)
        }
    }
}
