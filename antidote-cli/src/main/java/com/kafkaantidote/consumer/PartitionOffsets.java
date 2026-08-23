package com.kafkaantidote.consumer;

/**
 * One sample of a partition's offsets: the group's committed offset and the partition's current
 * log-end offset (high watermark). Internal to the consumer implementation.
 */
record PartitionOffsets(long committed, long logEnd) {

    /** Lag = messages produced but not yet consumed by the group. Never negative. */
    long lag() {
        return Math.max(0L, logEnd - committed);
    }
}
