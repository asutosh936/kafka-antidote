package com.kafkaantidote.core;

/**
 * Thrown by {@link MessageSource#fetchRaw} when the raw payload cannot be retrieved (Spec R2.4).
 *
 * <p>The message MUST be clear and actionable — it is shown to the operator as the primary output,
 * never as a raw stack trace. Part of the {@code core} boundary so components above it (the CLI) can
 * catch it without depending on any implementation.
 */
public final class MessageFetchException extends RuntimeException {

    public MessageFetchException(String message) {
        super(message);
    }

    public MessageFetchException(String message, Throwable cause) {
        super(message, cause);
    }
}
