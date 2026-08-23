package com.kafkaantidote.core;

import java.util.Objects;

/**
 * Identifies a consumer group on a cluster. Immutable value type (Spec §4.3).
 *
 * @param bootstrapServers Kafka bootstrap servers, e.g. {@code localhost:9092}
 * @param groupId          the consumer group id
 */
public record GroupCoordinates(String bootstrapServers, String groupId) {

    public GroupCoordinates {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(groupId, "groupId");
    }

    /** Human-readable form (Spec §4.3). */
    public String toHuman() {
        return "group '" + groupId + "' @ " + bootstrapServers;
    }
}
