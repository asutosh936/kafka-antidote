package com.kafkaantidote.core;

import java.util.List;

/**
 * The single architecture boundary (Spec §4.1, §4.4).
 *
 * <p>The CLI, payload inspector, classifier, and re-injection generator MUST depend only on this
 * interface and the value types in this package — never on Kafka consumer classes directly. That
 * is what makes the roadmap achievable: adding a future {@code StreamsMessageSource} or
 * {@code ConnectMessageSource} must touch zero files outside its own package plus one wiring line
 * (Spec §4.4). v0.1 ships exactly one implementation, {@code ConsumerMessageSource} (Spec §4.2).
 */
public interface MessageSource {

    /**
     * Find the partition(s) stuck on a poison pill (Spec R1). MUST be read-only: no commit, no seek,
     * no offset mutation (R1.4).
     */
    List<StuckPosition> findStuckPositions(GroupCoordinates group);

    /**
     * Fetch the raw bytes at a position WITHOUT routing them through the failing deserializer
     * (Spec R2.1). MUST be read-only.
     */
    RawMessage fetchRaw(TopicPartitionOffset position);

    /**
     * Build a safe re-injection plan for a corrected payload (Spec R4). Generating the plan is
     * read-only; it neither produces nor mutates anything (R4.6). The corrected payload is supplied
     * by the caller — the tool does not repair messages.
     */
    ReinjectionPlan planReinjection(RawMessage original, byte[] correctedPayload);
}
