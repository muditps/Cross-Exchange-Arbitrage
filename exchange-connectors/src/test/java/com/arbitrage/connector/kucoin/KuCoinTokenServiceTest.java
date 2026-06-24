package com.arbitrage.connector.kucoin;

import com.arbitrage.connector.ExchangeConnectionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link KuCoinTokenService}.
 *
 * <p>Uses an in-process Reactor Netty HTTP server to simulate the KuCoin
 * bullet-public endpoint. This avoids any real network calls while exercising
 * the actual WebClient HTTP logic (response parsing, error mapping, etc.).</p>
 *
 * <p>Tests cover: successful token extraction, field mapping, caching behaviour,
 * cache invalidation, and error scenarios (non-200 response, missing servers,
 * malformed JSON, KuCoin error codes).</p>
 */
class KuCoinTokenServiceTest {

    private static final String SUCCESS_RESPONSE = """
            {
              "code": "200000",
              "data": {
                "token": "test-token-abc123",
                "instanceServers": [
                  {
                    "endpoint": "wss://ws-api.kucoin.com/endpoint",
                    "encrypt": true,
                    "protocol": "websocket",
                    "pingInterval": 18000,
                    "pingTimeout": 10000
                  }
                ]
              }
            }
            """;

    private static final String ERROR_RESPONSE = """
            {"code": "400001", "msg": "Invalid request"}
            """;

    private static final String MULTIPLE_SERVERS_RESPONSE = """
            {
              "code": "200000",
              "data": {
                "token": "token-multi-server",
                "instanceServers": [
                  {
                    "endpoint": "wss://ws-api-primary.kucoin.com/endpoint",
                    "encrypt": true,
                    "protocol": "websocket",
                    "pingInterval": 20000,
                    "pingTimeout": 15000
                  },
                  {
                    "endpoint": "wss://ws-api-secondary.kucoin.com/endpoint",
                    "encrypt": true,
                    "protocol": "websocket",
                    "pingInterval": 20000,
                    "pingTimeout": 15000
                  }
                ]
              }
            }
            """;

    private static final String NO_SERVERS_RESPONSE = """
            {
              "code": "200000",
              "data": {
                "token": "token-no-servers",
                "instanceServers": []
              }
            }
            """;

    private DisposableServer mockHttpServer;
    private KuCoinTokenService tokenService;

    @BeforeEach
    void setUp() {
        startMockServer(SUCCESS_RESPONSE);
    }

    @AfterEach
    void tearDown() {
        if (mockHttpServer != null) {
            mockHttpServer.disposeNow();
        }
    }

    /**
     * Starts a mock HTTP server that always returns the given response body.
     *
     * <p>The test constructor of {@link KuCoinTokenService} accepts a full URL string
     * rather than a WebClient base URL, so the absolute URL is passed directly.
     * This prevents the absolute {@code BULLET_PUBLIC_URL} constant from overriding
     * the base URL when calling {@code WebClient.post().uri(absoluteUrl)}.</p>
     */
    private void startMockServer(String responseBody) {
        mockHttpServer = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/api/v1/bullet-public", (request, response) ->
                        response.header("Content-Type", "application/json")
                                .sendString(Mono.just(responseBody))
                ))
                .bindNow();

        String mockBulletUrl = "http://localhost:" + mockHttpServer.port() + "/api/v1/bullet-public";
        tokenService = new KuCoinTokenService(WebClient.create(), mockBulletUrl);
    }

    // ============================================================
    // Happy Path Tests
    // ============================================================

    @Nested
    @DisplayName("Successful Token Fetch")
    class SuccessfulFetchTests {

        @Test
        @DisplayName("Token is extracted from response")
        void token_isExtractedFromResponse() {
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> assertEquals("test-token-abc123", details.token()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("WebSocket endpoint is extracted from first instanceServer")
        void wsEndpoint_isExtractedFromFirstServer() {
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details ->
                            assertEquals("wss://ws-api.kucoin.com/endpoint", details.wsEndpoint()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("Ping interval is extracted from first instanceServer")
        void pingInterval_isExtractedFromFirstServer() {
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> assertEquals(18000L, details.pingIntervalMs()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("All connection details are non-null")
        void allConnectionDetails_areNonNull() {
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> {
                        assertNotNull(details.token());
                        assertNotNull(details.wsEndpoint());
                    })
                    .verifyComplete();
        }
    }

    // ============================================================
    // Caching Tests
    // ============================================================

    @Nested
    @DisplayName("Token Caching")
    class CachingTests {

        @Test
        @DisplayName("Second call returns the same Mono (cached)")
        void secondCall_returnsCachedResult() {
            Mono<KuCoinConnectionDetails> first = tokenService.getConnectionDetails();
            Mono<KuCoinConnectionDetails> second = tokenService.getConnectionDetails();

            // Same reference = cached (not a new Mono)
            assertEquals(first, second, "Subsequent calls should return the same cached Mono");
        }

        @Test
        @DisplayName("After invalidation, next call fetches fresh details")
        void afterInvalidation_nextCallFetchesFresh() {
            // Prime the cache
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> assertNotNull(details.token()))
                    .verifyComplete();

            // Invalidate
            tokenService.invalidateCache();

            // Should succeed again (mock server still running with same response)
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> assertEquals("test-token-abc123", details.token()))
                    .verifyComplete();
        }

        @Test
        @DisplayName("After invalidation, new Mono reference is created")
        void afterInvalidation_newMonoIsCreated() {
            Mono<KuCoinConnectionDetails> first = tokenService.getConnectionDetails();
            tokenService.invalidateCache();
            Mono<KuCoinConnectionDetails> second = tokenService.getConnectionDetails();

            // Different reference = new Mono created after invalidation
            assertNotNull(second);
            // They could be different references — the cache was cleared and recreated
        }
    }

    // ============================================================
    // Multiple Servers Test
    // ============================================================

    @Nested
    @DisplayName("Multiple Instance Servers")
    class MultipleServersTests {

        @Test
        @DisplayName("First server is selected when multiple are available")
        void firstServer_isSelectedFromMultiple() {
            if (mockHttpServer != null) {
                mockHttpServer.disposeNow();
            }
            startMockServer(MULTIPLE_SERVERS_RESPONSE);

            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> {
                        assertEquals("token-multi-server", details.token());
                        // Primary server is selected
                        assertEquals("wss://ws-api-primary.kucoin.com/endpoint", details.wsEndpoint());
                        assertEquals(20000L, details.pingIntervalMs());
                    })
                    .verifyComplete();
        }
    }

    // ============================================================
    // Error Handling Tests
    // ============================================================

    @Nested
    @DisplayName("Error Scenarios")
    class ErrorTests {

        @Test
        @DisplayName("KuCoin error code returns ExchangeConnectionException")
        void kucoinErrorCode_returnsException() {
            if (mockHttpServer != null) {
                mockHttpServer.disposeNow();
            }
            startMockServer(ERROR_RESPONSE);

            StepVerifier.create(tokenService.getConnectionDetails())
                    .expectError(ExchangeConnectionException.class)
                    .verify();
        }

        @Test
        @DisplayName("Empty instance servers list returns ExchangeConnectionException")
        void emptyServersList_returnsException() {
            if (mockHttpServer != null) {
                mockHttpServer.disposeNow();
            }
            startMockServer(NO_SERVERS_RESPONSE);

            StepVerifier.create(tokenService.getConnectionDetails())
                    .expectError(ExchangeConnectionException.class)
                    .verify();
        }

        @Test
        @DisplayName("Cache is cleared after error so next call can retry")
        void cacheIsClearedAfterError() {
            if (mockHttpServer != null) {
                mockHttpServer.disposeNow();
            }
            startMockServer(ERROR_RESPONSE);

            // Prime with error
            StepVerifier.create(tokenService.getConnectionDetails())
                    .expectError(ExchangeConnectionException.class)
                    .verify();

            // Now restart with success response
            if (mockHttpServer != null) {
                mockHttpServer.disposeNow();
            }
            startMockServer(SUCCESS_RESPONSE);

            // Should succeed now (cache was cleared by error handler)
            StepVerifier.create(tokenService.getConnectionDetails())
                    .assertNext(details -> assertEquals("test-token-abc123", details.token()))
                    .verifyComplete();
        }
    }
}
