package com.kafkaantidote.payload;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.util.Map;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/**
 * Property-based hardening (Implementation Plan §2 / Phase 4): thousands of random malformed
 * payloads must never crash the classifier, and it must always return a labeled heuristic.
 */
class ClassifierPropertyTest {

    @Property(tries = 2000)
    void neverCrashesAndAlwaysReturnsALabeledHeuristic(@ForAll byte[] payload) {
        RawMessage raw = new RawMessage(
                new TopicPartitionOffset("t", 0, 0), payload, null, Map.of(), 0L, payload.length);

        FailureClassification c = FailureClassifier.classify(raw);

        assertNotNull(c, "classification is never null");
        assertNotNull(c.category(), "always has a category");
        assertNotNull(c.confidence(), "always labeled with a confidence");
        assertTrue(c.toHuman().contains("[heuristic:"), "always rendered as a heuristic");
    }
}
