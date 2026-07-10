package com.arbitrage.connector.nse;

/**
 * Immutable value object holding the Angel One SmartAPI authentication credentials
 * obtained after a successful login.
 *
 * <p>Angel One's auth flow returns two distinct tokens:
 * <ul>
 *   <li>{@link #jwtToken} — Bearer token for REST API calls and the WebSocket
 *       {@code Authorization} header. Short-lived; expires after the trading session.</li>
 *   <li>{@link #feedToken} — Separate token required specifically for the SmartStream
 *       WebSocket connection via the {@code x-feed-token} header. Issued alongside the
 *       JWT and has the same lifetime.</li>
 * </ul>
 *
 * <p>Both tokens are required to open the WebSocket connection. Using only the JWT
 * without the feed token results in a WebSocket 403 rejection.</p>
 *
 * <p>This record is produced by {@link NseTokenService} and consumed by
 * {@link NseConnector} to build the authenticated WebSocket URI headers.</p>
 *
 * @param jwtToken    Bearer JWT for REST calls and WebSocket Authorization header.
 *                    Prefixed with "Bearer " by Angel One in the response; stored
 *                    as-is and passed directly to the Authorization header.
 * @param feedToken   SmartStream-specific feed token for the x-feed-token header.
 * @param clientCode  The Angel One client code (user identifier) needed as the
 *                    x-client-code WebSocket header.
 */
public record NseAuthCredentials(String jwtToken, String feedToken, String clientCode) {

    /**
     * Compact constructor validating that none of the credential fields are blank.
     * Blank credentials cause a silent 403 on the WebSocket rather than an obvious
     * startup error — catching this early produces a clearer failure message.
     */
    public NseAuthCredentials {
        if (jwtToken == null || jwtToken.isBlank()) {
            throw new IllegalArgumentException("jwtToken must not be blank");
        }
        if (feedToken == null || feedToken.isBlank()) {
            throw new IllegalArgumentException("feedToken must not be blank");
        }
        if (clientCode == null || clientCode.isBlank()) {
            throw new IllegalArgumentException("clientCode must not be blank");
        }
    }
}
