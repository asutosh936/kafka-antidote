package com.kafkaantidote.reinject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.ReinjectionPlan;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Safety-contract tests for the generated re-injection script (Spec R4). These assert on the
 * generated source directly — no broker — so the R4.2–R4.6 guarantees are pinned regardless of I/O.
 */
class ReinjectionScriptGeneratorTest {

    private RawMessage original() {
        return new RawMessage(
                new TopicPartitionOffset("orders", 3, 84213),
                "old-value".getBytes(StandardCharsets.UTF_8),
                "order-42".getBytes(StandardCharsets.UTF_8),
                Map.of("content-type", "application/json".getBytes(StandardCharsets.UTF_8)),
                1_700_000_000_000L, 9);
    }

    private ReinjectionPlan plan(byte[] corrected) {
        return ReinjectionScriptGenerator.generate(original(), corrected, "localhost:9092");
    }

    @Test
    void planTargetsTheExactOriginalPosition() {
        ReinjectionPlan p = plan("fixed".getBytes(StandardCharsets.UTF_8));
        assertEquals(new TopicPartitionOffset("orders", 3, 84213), p.target());
        assertEquals("java", p.language());
    }

    @Test
    void targetsExactPartitionWithExplicitPartitionConstructor() { // R4.2
        String s = plan("fixed".getBytes(StandardCharsets.UTF_8)).scriptContent();
        assertTrue(s.contains("PARTITION = 3"), "embeds the exact partition");
        assertTrue(s.contains("new ProducerRecord<>(TOPIC, PARTITION, key, value)"),
                "uses the explicit-partition ProducerRecord constructor, not key-hash");
        assertTrue(s.contains("SAFETY CHECK FAILED"), "asserts at runtime it landed on PARTITION");
    }

    @Test
    void neverSkipsSeeksOrCommitsOffsets() { // R4.3 / R4.6
        String s = plan("fixed".getBytes(StandardCharsets.UTF_8)).scriptContent();
        assertFalse(s.contains("KafkaConsumer"), "no consumer -> cannot touch offsets");
        assertFalse(s.contains(".seek("), "must not seek");
        assertFalse(s.contains("commitSync"), "must not commit");
        assertFalse(s.contains("commitAsync"), "must not commit");
    }

    @Test
    void defaultsToDryRunAndRequiresExplicitConfirmation() { // R4.4
        String s = plan("fixed".getBytes(StandardCharsets.UTF_8)).scriptContent();
        assertTrue(s.contains("--produce") && s.contains("--yes"),
                "requires both flags to actually send");
        assertTrue(s.contains("DRY RUN"), "has a dry-run mode");
        assertTrue(s.contains("has(args, \"--produce\") && has(args, \"--yes\")"),
                "produces only when both flags are present");
    }

    @Test
    void carriesAWarningHeader() { // R4.5
        String s = plan("fixed".getBytes(StandardCharsets.UTF_8)).scriptContent();
        assertTrue(s.contains("WARNING"), "warns");
        assertTrue(s.toLowerCase().contains("review"), "tells the human to review");
        assertTrue(s.toLowerCase().contains("root cause"), "tells the human to fix the root cause");
        assertTrue(s.toLowerCase().contains("does not") && s.toLowerCase().contains("unstick"),
                "states it does not unstick the group (R4.6)");
    }

    @Test
    void embedsCorrectedPayloadAndPreservesOriginalKeyAndHeaders() { // R4.1
        byte[] corrected = "{\"amount\":42}".getBytes(StandardCharsets.UTF_8);
        String s = plan(corrected).scriptContent();
        assertTrue(s.contains(Base64.getEncoder().encodeToString(corrected)),
                "embeds the corrected payload as base64");
        assertTrue(s.contains(Base64.getEncoder().encodeToString("order-42".getBytes(StandardCharsets.UTF_8))),
                "preserves the original key");
        assertTrue(s.contains("content-type"), "preserves original headers");
    }

    @Test
    void handlesNullKey() {
        RawMessage noKey = new RawMessage(new TopicPartitionOffset("orders", 0, 1),
                "v".getBytes(StandardCharsets.UTF_8), null, Map.of(), 1L, 1);
        String s = ReinjectionScriptGenerator.generate(noKey, "x".getBytes(StandardCharsets.UTF_8),
                "localhost:9092").scriptContent();
        assertTrue(s.contains("KEY_B64   = null"), "renders a null key as the literal null: " + s);
    }
}
