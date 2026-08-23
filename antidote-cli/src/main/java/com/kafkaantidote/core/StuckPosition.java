package com.kafkaantidote.core;

import java.util.Objects;

/**
 * A partition diagnosed as stuck, with the numbers R1.2 requires the tool to report:
 * topic, partition, committed offset, log-end offset, lag. Immutable value type (Spec §4.3).
 *
 * <p>The poison message sits at {@code committedOffset} — the next offset the group would read
 * but cannot get past.
 */
public record StuckPosition(String topic, int partition, long committedOffset,
                            long logEndOffset, long lag) {

    public StuckPosition {
        Objects.requireNonNull(topic, "topic");
    }

    /** The exact position of the poison message (the offset the group is stuck on). */
    public TopicPartitionOffset poisonPosition() {
        return new TopicPartitionOffset(topic, partition, committedOffset);
    }

    public String toHuman() {
        return topic + "-" + partition
                + ": committed=" + committedOffset
                + " logEnd=" + logEndOffset
                + " lag=" + lag;
    }
}
