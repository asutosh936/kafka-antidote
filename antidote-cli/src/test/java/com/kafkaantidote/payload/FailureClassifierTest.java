package com.kafkaantidote.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.FailureClassification.Category;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.TopicPartitionOffset;
import com.kafkaantidote.fixtures.PoisonFixture;
import com.kafkaantidote.fixtures.PoisonFixtureGenerator;
import com.kafkaantidote.fixtures.PoisonType;
import org.junit.jupiter.api.Test;

/**
 * Classifier heuristics (Spec R3), driven by the poison-fixture corpus. Every verdict must be a
 * labeled heuristic (R3.2) — never asserted as certain.
 */
class FailureClassifierTest {

    private final PoisonFixtureGenerator generator = new PoisonFixtureGenerator();

    private RawMessage rawFrom(PoisonType type) {
        PoisonFixture f = generator.generate(type);
        return new RawMessage(new TopicPartitionOffset("t", 0, 0),
                f.value(), f.key(), f.headers(), 0L, f.value().length);
    }

    @Test
    void oversizedIsClassifiedAsOversized() {
        assertEquals(Category.OVERSIZED_MESSAGE, FailureClassifier.classify(rawFrom(PoisonType.OVERSIZED)).category());
    }

    @Test
    void confluentFramedAvroIsClassifiedAsSchemaMismatch() {
        // Both the truncated-Avro and version-mismatch fixtures are Confluent-framed; from bytes
        // alone we can only report the wire-format heuristic (R3.3).
        assertEquals(Category.SCHEMA_VERSION_MISMATCH,
                FailureClassifier.classify(rawFrom(PoisonType.TRUNCATED_AVRO)).category());
        assertEquals(Category.SCHEMA_VERSION_MISMATCH,
                FailureClassifier.classify(rawFrom(PoisonType.SCHEMA_VERSION_MISMATCH)).category());
    }

    @Test
    void validJsonIsClassifiedAsUnknownWithHelpfulDetail() {
        // Cannot prove a type mismatch without the expected schema, so it stays UNKNOWN (honest).
        FailureClassification c = FailureClassifier.classify(rawFrom(PoisonType.JSON_WRONG_TYPE));
        assertEquals(Category.UNKNOWN, c.category());
        assertFalse(c.detail().isBlank());
    }

    @Test
    void everyPoisonTypeGetsALabeledHeuristic() {
        // Phase-2 DoD: every poison type is classified with a labeled heuristic, never null/certain.
        for (PoisonType type : PoisonType.values()) {
            FailureClassification c = FailureClassifier.classify(rawFrom(type));
            assertNotNull(c.category(), type + " has a category");
            assertNotNull(c.confidence(), type + " has a confidence level");
            assertFalse(c.detail().isBlank(), type + " has detail");
            // The rendered form is explicitly marked a heuristic (R3.2).
            assertFalse(c.toHuman().isBlank());
            org.junit.jupiter.api.Assertions.assertTrue(
                    c.toHuman().contains("[heuristic:"), type + " labeled as heuristic");
        }
    }

    @Test
    void plainBinaryIsClassifiedAsDeserializationFailure() {
        byte[] binary = {(byte) 0xFF, (byte) 0xFE, 0x01, 0x02, (byte) 0x99};
        RawMessage raw = new RawMessage(new TopicPartitionOffset("t", 0, 0),
                binary, null, java.util.Map.of(), 0L, binary.length);
        assertEquals(Category.DESERIALIZATION_FAILURE, FailureClassifier.classify(raw).category());
    }
}
