package com.arbitrage.connector.reconnect;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.connector.ExchangeConnectionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ExponentialBackoffReconnectStrategy}.
 *
 * <p>Verifies configuration storage, retry behaviour (count, exception type on
 * exhaustion), and string representation. Uses a deliberately failing {@link Mono}
 * to exercise the retry spec — each attempt increments a counter, and we verify
 * the total number of attempts matches the configured maximum.</p>
 *
 * <p><b>Note on delay testing:</b> We do NOT test actual delay durations (1s, 2s, 4s)
 * because Reactor's {@code Retry.backoff()} adds jitter, making exact timing
 * non-deterministic. Instead, we use {@link StepVerifier#withVirtualTime} where
 * needed and verify the retry COUNT and EXCEPTION TYPE, which are deterministic.</p>
 */
class ExponentialBackoffReconnectStrategyTest {

    private static final ExchangeId EXCHANGE = ExchangeId.BINANCE;
    private static final Duration INITIAL_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_DELAY = Duration.ofSeconds(1);
    private static final long MAX_ATTEMPTS = 3;

    private ExponentialBackoffReconnectStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new ExponentialBackoffReconnectStrategy(
                EXCHANGE, INITIAL_DELAY, MAX_DELAY, MAX_ATTEMPTS);
    }

    @Nested
    @DisplayName("Configuration")
    class ConfigurationTests {

        @Test
        @DisplayName("All parameters are stored correctly")
        void parametersStoredCorrectly() {
            assertEquals(EXCHANGE, strategy.getExchangeId());
            assertEquals(INITIAL_DELAY, strategy.getInitialDelay());
            assertEquals(MAX_DELAY, strategy.getMaxDelay());
            assertEquals(MAX_ATTEMPTS, strategy.getMaxAttempts());
        }

        @Test
        @DisplayName("Different exchanges have different strategy identities")
        void differentExchanges_differentStrategies() {
            ExponentialBackoffReconnectStrategy bybitStrategy =
                    new ExponentialBackoffReconnectStrategy(
                            ExchangeId.BYBIT, Duration.ofSeconds(2), Duration.ofSeconds(60), 5);

            assertEquals(ExchangeId.BYBIT, bybitStrategy.getExchangeId());
            assertEquals(Duration.ofSeconds(2), bybitStrategy.getInitialDelay());
        }
    }

    @Nested
    @DisplayName("Retry Behaviour")
    class RetryBehaviourTests {

        @Test
        @DisplayName("buildRetrySpec returns a non-null RetryBackoffSpec")
        void buildRetrySpec_returnsNonNull() {
            RetryBackoffSpec spec = strategy.buildRetrySpec();
            assertNotNull(spec);
        }

        @Test
        @DisplayName("Retries exactly maxAttempts times before exhaustion")
        void retriesExactlyMaxAttempts() {
            AtomicInteger attemptCounter = new AtomicInteger(0);
            RetryBackoffSpec spec = strategy.buildRetrySpec();

            // Create a Mono that always fails — forces all retries to be used
            Mono<String> failingMono = Mono.defer(() -> {
                attemptCounter.incrementAndGet();
                return Mono.error(new RuntimeException("Connection refused"));
            });

            StepVerifier.withVirtualTime(() -> failingMono.retryWhen(spec))
                    .thenAwait(Duration.ofMinutes(5))  // advance virtual time past all delays
                    .expectErrorSatisfies(error -> {
                        assertInstanceOf(ExchangeConnectionException.class, error);
                        ExchangeConnectionException ex = (ExchangeConnectionException) error;
                        assertEquals(EXCHANGE, ex.getExchangeId());
                        assertTrue(ex.getMessage().contains("reconnection attempts exhausted"));
                    })
                    .verify();

            // 1 initial attempt + maxAttempts retries = maxAttempts + 1 total calls
            assertEquals(MAX_ATTEMPTS + 1, attemptCounter.get(),
                    "Should make 1 initial + " + MAX_ATTEMPTS + " retry attempts");
        }

        @Test
        @DisplayName("Exhaustion exception wraps the last failure cause")
        void exhaustionException_wrapsLastFailure() {
            RetryBackoffSpec spec = strategy.buildRetrySpec();

            Mono<String> failingMono = Mono.error(new RuntimeException("Network unreachable"));

            StepVerifier.withVirtualTime(() -> failingMono.retryWhen(spec))
                    .thenAwait(Duration.ofMinutes(5))
                    .expectErrorSatisfies(error -> {
                        assertInstanceOf(ExchangeConnectionException.class, error);
                        ExchangeConnectionException ex = (ExchangeConnectionException) error;
                        assertNotNull(ex.getCause(), "Should wrap the original exception");
                        assertEquals("Network unreachable", ex.getCause().getMessage());
                    })
                    .verify();
        }

        @Test
        @DisplayName("Exhaustion exception message contains exchange ID")
        void exhaustionException_containsExchangeId() {
            RetryBackoffSpec spec = strategy.buildRetrySpec();

            Mono<String> failingMono = Mono.error(new RuntimeException("timeout"));

            StepVerifier.withVirtualTime(() -> failingMono.retryWhen(spec))
                    .thenAwait(Duration.ofMinutes(5))
                    .expectErrorSatisfies(error -> {
                        assertInstanceOf(ExchangeConnectionException.class, error);
                        assertTrue(error.getMessage().contains(EXCHANGE.name()),
                                "Exception message should contain exchange name");
                    })
                    .verify();
        }

        @Test
        @DisplayName("Successful Mono does not trigger any retries")
        void successfulMono_noRetries() {
            AtomicInteger attemptCounter = new AtomicInteger(0);
            RetryBackoffSpec spec = strategy.buildRetrySpec();

            Mono<String> successMono = Mono.defer(() -> {
                attemptCounter.incrementAndGet();
                return Mono.just("connected");
            });

            StepVerifier.create(successMono.retryWhen(spec))
                    .expectNext("connected")
                    .verifyComplete();

            assertEquals(1, attemptCounter.get(), "Successful Mono should only be called once");
        }

        @Test
        @DisplayName("Transient failure followed by success retries and recovers")
        void transientFailure_recoversOnRetry() {
            AtomicInteger attemptCounter = new AtomicInteger(0);
            RetryBackoffSpec spec = strategy.buildRetrySpec();

            // Fail twice, then succeed on third attempt
            Mono<String> transientMono = Mono.defer(() -> {
                int attempt = attemptCounter.incrementAndGet();
                if (attempt <= 2) {
                    return Mono.error(new RuntimeException("Transient error #" + attempt));
                }
                return Mono.just("recovered");
            });

            StepVerifier.withVirtualTime(() -> transientMono.retryWhen(spec))
                    .thenAwait(Duration.ofMinutes(5))
                    .expectNext("recovered")
                    .verifyComplete();

            assertEquals(3, attemptCounter.get(),
                    "Should fail twice and succeed on third attempt");
        }
    }

    @Nested
    @DisplayName("Single Retry Configuration")
    class SingleRetryTests {

        @Test
        @DisplayName("maxAttempts=1 retries once then exhausts")
        void singleRetry_exhaustsAfterOne() {
            ExponentialBackoffReconnectStrategy singleRetry =
                    new ExponentialBackoffReconnectStrategy(
                            EXCHANGE, INITIAL_DELAY, MAX_DELAY, 1);
            AtomicInteger counter = new AtomicInteger(0);

            Mono<String> failingMono = Mono.defer(() -> {
                counter.incrementAndGet();
                return Mono.error(new RuntimeException("fail"));
            });

            StepVerifier.withVirtualTime(() -> failingMono.retryWhen(singleRetry.buildRetrySpec()))
                    .thenAwait(Duration.ofMinutes(1))
                    .expectError(ExchangeConnectionException.class)
                    .verify();

            assertEquals(2, counter.get(), "1 initial + 1 retry = 2 total attempts");
        }
    }

    @Nested
    @DisplayName("String Representation")
    class ToStringTests {

        @Test
        @DisplayName("toString contains exchange ID and all parameters")
        void toString_containsAllParameters() {
            String result = strategy.toString();

            assertTrue(result.contains(EXCHANGE.name()), "Should contain exchange name");
            assertTrue(result.contains("initialDelay"), "Should mention initialDelay");
            assertTrue(result.contains("maxDelay"), "Should mention maxDelay");
            assertTrue(result.contains("maxAttempts"), "Should mention maxAttempts");
            assertTrue(result.contains(String.valueOf(MAX_ATTEMPTS)), "Should contain maxAttempts value");
        }
    }
}
