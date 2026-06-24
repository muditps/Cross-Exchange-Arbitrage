package com.arbitrage.connector.kucoin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Wire-format POJO for KuCoin's {@code POST /api/v1/bullet-public} REST response.
 *
 * <p>KuCoin wraps all REST responses in a standard envelope with a {@code code} and
 * {@code data} field. The {@code code} is {@code "200000"} (a string, not an integer)
 * on success. The {@code data} object contains the WebSocket token and a list of
 * available instance servers — we always use the first server in the list.</p>
 *
 * <p><b>Full wire format:</b></p>
 * <pre>
 * {
 *   "code": "200000",
 *   "data": {
 *     "token": "2neAiuYvAU37qqZHBBkIAuKomJKVKCsN0o6T1Yy9O3M=",
 *     "instanceServers": [
 *       {
 *         "endpoint": "wss://ws-api.kucoin.com/endpoint",
 *         "encrypt": true,
 *         "protocol": "websocket",
 *         "pingInterval": 18000,
 *         "pingTimeout": 10000
 *       }
 *     ]
 *   }
 * }
 * </pre>
 *
 * <p><b>Why instanceServers is a list:</b> KuCoin may return multiple WebSocket servers
 * for redundancy or regional routing. We use the first server since they are equivalent
 * for a client that only needs one connection.</p>
 *
 * @see KuCoinTokenService for how this response is parsed and converted to
 *      {@link KuCoinConnectionDetails}
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KuCoinBulletPublicResponse {

    /**
     * KuCoin's response code. {@code "200000"} means success.
     * Note: this is a String, not an integer — KuCoin uses string codes.
     */
    private String code;

    /**
     * The response data containing the token and instance servers.
     */
    private BulletData data;

    /**
     * Returns true if the response indicates success ({@code "200000"}).
     *
     * @return true if the request succeeded
     */
    public boolean isSuccess() {
        return "200000".equals(code);
    }

    /**
     * Data envelope containing the WebSocket token and server list.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BulletData {

        /**
         * The temporary WebSocket authentication token.
         * Must be appended to the WebSocket URL as {@code ?token=<TOKEN>}.
         * Valid for approximately 24 hours for public (unauthenticated) tokens.
         */
        private String token;

        /**
         * Available WebSocket server instances. We always use the first entry.
         */
        private List<InstanceServer> instanceServers;

        /**
         * Returns the first available instance server, or {@code null} if the list is empty.
         *
         * @return the primary instance server to connect to
         */
        public InstanceServer getPrimaryServer() {
            if (instanceServers == null || instanceServers.isEmpty()) {
                return null;
            }
            return instanceServers.get(0);
        }
    }

    /**
     * A single KuCoin WebSocket server instance.
     */
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InstanceServer {

        /**
         * The WebSocket base endpoint URL (without token).
         * Example: {@code wss://ws-api.kucoin.com/endpoint}
         */
        private String endpoint;

        /**
         * Whether this endpoint uses TLS encryption. Always {@code true} in production.
         */
        private boolean encrypt;

        /**
         * Protocol identifier. Always {@code "websocket"} for WebSocket endpoints.
         */
        private String protocol;

        /**
         * Client heartbeat interval in milliseconds.
         * The client must send a {@code {"id":"...","type":"ping"}} message every
         * {@code pingInterval} milliseconds or KuCoin will disconnect.
         * Typical value: 18000 (18 seconds).
         */
        @JsonProperty("pingInterval")
        private long pingInterval;

        /**
         * Server-side ping timeout in milliseconds.
         * If the server doesn't receive a pong within this time after sending a ping,
         * it considers the connection dead. Typical value: 10000 (10 seconds).
         */
        @JsonProperty("pingTimeout")
        private long pingTimeout;
    }
}
