package com.arbitrage.connector;

import com.arbitrage.common.model.ExchangeId;
import lombok.Getter;

/**
 * Exception thrown when an exchange connector encounters a connection failure.
 *
 * <p>This exception carries the {@link ExchangeId} of the affected exchange, enabling
 * structured error handling and contextual logging in downstream error handlers.
 * Since it extends {@link RuntimeException} (unchecked), it is compatible with
 * Project Reactor's error channel — errors flow through {@code Flux.onError()}
 * rather than being thrown across thread boundaries.</p>
 *
 * <p>Common causes: WebSocket handshake failure, DNS resolution error, exchange
 * rate limiting, unexpected disconnection after all reconnection attempts exhausted.</p>
 */
@Getter
public class ExchangeConnectionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * The exchange where the connection failure occurred.
     */
    private final ExchangeId exchangeId;

    /**
     * Creates an exception for a connection failure on the specified exchange.
     *
     * @param exchangeId the exchange where the failure occurred
     * @param message    a human-readable description of the failure
     */
    public ExchangeConnectionException(ExchangeId exchangeId, String message) {
        super(message);
        this.exchangeId = exchangeId;
    }

    /**
     * Creates an exception for a connection failure on the specified exchange,
     * wrapping an underlying cause (e.g., an I/O or WebSocket protocol error).
     *
     * @param exchangeId the exchange where the failure occurred
     * @param message    a human-readable description of the failure
     * @param cause      the underlying exception that triggered this failure
     */
    public ExchangeConnectionException(ExchangeId exchangeId, String message, Throwable cause) {
        super(message, cause);
        this.exchangeId = exchangeId;
    }
}
