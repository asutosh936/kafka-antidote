package com.kafkaantidote.cli;

import com.kafkaantidote.consumer.ConsumerMessageSource;
import com.kafkaantidote.core.MessageSource;
import java.time.Duration;

/**
 * The single wiring point where a concrete {@link MessageSource} implementation is chosen (Spec
 * §4.4). Commands depend only on the {@code MessageSource} interface and this factory — never on a
 * concrete implementation — so a future {@code StreamsMessageSource} or {@code ConnectMessageSource}
 * is added by touching its own package plus one method here, and nothing else.
 */
final class MessageSources {

    private MessageSources() {
    }

    /** The v0.1 implementation: plain Kafka consumers. */
    static MessageSource forConsumer(int samples, Duration pollInterval) {
        return new ConsumerMessageSource(samples, pollInterval);
    }
}
