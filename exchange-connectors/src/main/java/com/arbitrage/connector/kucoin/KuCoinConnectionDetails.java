package com.arbitrage.connector.kucoin;

/**
 * Holds the connection details obtained from KuCoin's bullet-public REST endpoint.
 *
 * <p>KuCoin does not accept direct WebSocket connections. Instead, a POST to
 * {@code https://api.kucoin.com/api/v1/bullet-public} returns:
 * <ul>
 *   <li>A short-lived <b>token</b> that must be appended to the WebSocket URL</li>
 *   <li>The actual <b>WebSocket endpoint URL</b> to connect to (varies by region)</li>
 *   <li>The <b>ping interval</b> in milliseconds — the client must send heartbeats
 *       at this cadence or KuCoin disconnects</li>
 * </ul>
 *
 * <p><b>Latency implication:</b> This REST call adds a one-time overhead before the
 * WebSocket is established. For live trading, this is acceptable because the token
 * is cached and reused for ~24 hours. On reconnection, the cache is invalidated and
 * a fresh token is fetched before re-establishing the WebSocket. This adds one extra
 * HTTP round-trip (~50–200ms) on the reconnect path — acceptable since reconnects
 * are rare and the alternative (reusing an expired token) fails immediately.</p>
 *
 * @param token          the temporary authentication token for the WebSocket URL
 * @param wsEndpoint     the base WebSocket endpoint URL (without token query param)
 * @param pingIntervalMs milliseconds between required client heartbeat pings
 * @see KuCoinTokenService for how this is obtained and cached
 * @see KuCoinConnector for how the WS URL is constructed using this record
 */
public record KuCoinConnectionDetails(String token, String wsEndpoint, long pingIntervalMs) {
}
