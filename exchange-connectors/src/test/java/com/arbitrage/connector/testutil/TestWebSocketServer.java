package com.arbitrage.connector.testutil;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * A reusable WebSocket server for integration tests that sends configurable
 * messages to connected clients.
 *
 * <p><b>Why this exists:</b> Integration tests for all exchange connectors
 * (Binance, Bybit, KuCoin) need a local WebSocket server that simulates the
 * exchange. Instead of duplicating server setup in every test class, this
 * utility encapsulates the pattern: start server on a random port, send
 * messages with configurable delay, expose the port for the connector.</p>
 *
 * <p><b>Usage:</b></p>
 * <pre>
 * TestWebSocketServer server = new TestWebSocketServer(
 *     List.of("{\"result\":null,\"id\":1}", bookTickerJson),
 *     Duration.ofMillis(50)
 * );
 * server.start();
 * // connector.connect("ws://localhost:" + server.getPort() + "/ws")
 * server.stop();
 * </pre>
 *
 * <p><b>Reusability:</b> This is exchange-agnostic — it sends whatever messages
 * you give it. Binance tests pass bookTicker JSON, Bybit tests pass their ticker
 * format, KuCoin tests pass their format. The server doesn't parse or validate
 * the messages — it just sends them over WebSocket.</p>
 *
 * <p><b>Random port:</b> Binds to port 0, which lets the OS assign an available
 * port. This prevents port conflicts when multiple tests run in parallel or when
 * another service is using a fixed port.</p>
 */
public class TestWebSocketServer {

    private final List<String> messages;
    private final Duration delayBetweenMessages;
    private DisposableServer server;

    /**
     * Creates a test WebSocket server with the given messages and delay.
     *
     * @param messages              ordered list of messages to send to each client
     * @param delayBetweenMessages  delay between consecutive messages (simulates network timing)
     */
    public TestWebSocketServer(List<String> messages, Duration delayBetweenMessages) {
        this.messages = messages;
        this.delayBetweenMessages = delayBetweenMessages;
    }

    /**
     * Starts the WebSocket server on a random port.
     *
     * <p>The server handles connections at the {@code /ws} path. For each connected
     * client, it sends all configured messages with the configured delay between them.
     * Incoming messages from the client (e.g., subscription requests) are received but
     * ignored — the test fixtures are sent regardless.</p>
     */
    public void start() {
        server = HttpServer.create()
                .port(0)
                .route(routes -> routes.ws("/ws", (wsInbound, wsOutbound) -> {
                    Mono<Void> receive = wsInbound.receive().then();

                    Flux<String> responses = Flux.fromIterable(messages)
                            .delayElements(delayBetweenMessages);

                    Mono<Void> send = wsOutbound.sendString(responses).then();

                    return Mono.when(send, receive);
                }))
                .bindNow();
    }

    /**
     * Returns the port the server is listening on.
     *
     * @return the assigned port number
     * @throws IllegalStateException if the server has not been started
     */
    public int getPort() {
        if (server == null) {
            throw new IllegalStateException("Server not started. Call start() first.");
        }
        return server.port();
    }

    /**
     * Returns the full WebSocket URI for connecting to this server.
     *
     * @return WebSocket URI string (e.g., "ws://localhost:54321/ws")
     */
    public String getWsUri() {
        return "ws://localhost:" + getPort() + "/ws";
    }

    /**
     * Stops the server and releases the port.
     */
    public void stop() {
        if (server != null) {
            server.disposeNow();
            server = null;
        }
    }

    /**
     * Loads a JSON fixture file from the test resources directory.
     *
     * <p>Fixture files are stored in {@code src/test/resources/fixtures/{exchange}/}
     * and contain real recorded WebSocket messages.</p>
     *
     * @param resourcePath path relative to the classpath (e.g., "fixtures/binance/book-ticker-btcusdt-01.json")
     * @return the file contents as a string
     * @throws IOException if the file cannot be read
     */
    public static String loadFixture(String resourcePath) throws IOException {
        Path path = Path.of("src/test/resources", resourcePath);
        return Files.readString(path, StandardCharsets.UTF_8).trim();
    }
}
