package com.arbitrage.connector;

import com.arbitrage.common.model.ExchangeId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for {@link ExchangeConnectionException}.
 * Verifies that the exception correctly carries exchange context for structured error handling.
 */
class ExchangeConnectionExceptionTest {

    @Test
    @DisplayName("Constructor with message sets exchangeId and message correctly")
    void constructorWithMessage_setsExchangeIdAndMessage() {
        ExchangeConnectionException exception =
                new ExchangeConnectionException(ExchangeId.BINANCE, "WebSocket handshake failed");

        assertEquals(ExchangeId.BINANCE, exception.getExchangeId());
        assertEquals("WebSocket handshake failed", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Constructor with cause wraps the underlying exception")
    void constructorWithCause_wrapsUnderlyingException() {
        RuntimeException cause = new RuntimeException("Connection refused");
        ExchangeConnectionException exception =
                new ExchangeConnectionException(ExchangeId.BYBIT, "Failed to connect", cause);

        assertEquals(ExchangeId.BYBIT, exception.getExchangeId());
        assertEquals("Failed to connect", exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    @DisplayName("getExchangeId returns correct exchange for each supported exchange")
    void getExchangeId_returnsCorrectExchange() {
        for (ExchangeId exchangeId : ExchangeId.values()) {
            ExchangeConnectionException exception =
                    new ExchangeConnectionException(exchangeId, "test");

            assertEquals(exchangeId, exception.getExchangeId(),
                    "ExchangeId should match for " + exchangeId.name());
        }
    }

    @Test
    @DisplayName("Exception is an instance of RuntimeException for Reactor compatibility")
    void exceptionIsUnchecked_forReactorCompatibility() {
        ExchangeConnectionException exception =
                new ExchangeConnectionException(ExchangeId.KUCOIN, "test");

        assertEquals(true, exception instanceof RuntimeException,
                "Must extend RuntimeException for Reactor error channel compatibility");
    }
}
