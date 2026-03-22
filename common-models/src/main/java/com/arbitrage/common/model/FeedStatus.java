package com.arbitrage.common.model;

/**
 * Health status of a WebSocket feed from an exchange.
 *
 * <p>In trading systems, a stale feed is worse than a dead feed:
 * a dead feed is obvious (you get an error), but a stale feed silently
 * serves old data that looks correct but is dangerously wrong.
 * Feed health monitoring catches this.
 */
public enum FeedStatus {

    /** WebSocket connected and actively receiving messages. */
    CONNECTED,

    /** Connection lost, attempting to reconnect with exponential backoff. */
    RECONNECTING,

    /** Connected but no message received within the staleness threshold. */
    STALE,

    /** Connection lost and all reconnection attempts exhausted. */
    DISCONNECTED
}
