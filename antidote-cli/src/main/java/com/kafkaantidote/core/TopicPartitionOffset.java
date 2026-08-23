package com.kafkaantidote.core;

import java.util.Objects;

/**
 * A precise position in the log: topic, partition, offset. Immutable value type (Spec §4.3).
 */
public record TopicPartitionOffset(String topic, int partition, long offset) {

    public TopicPartitionOffset {
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0, was " + partition);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        }
    }

    public String toHuman() {
        return topic + "-" + partition + "@" + offset;
    }
}
