package com.kafkaantidote.fixtures;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Phase-0 Definition of Done: the generator emits all five poison types (Implementation Plan, Phase 0).
 */
class PoisonFixtureGeneratorTest {

    private final PoisonFixtureGenerator generator = new PoisonFixtureGenerator();

    @Test
    void emitsAllFivePoisonTypes() {
        List<PoisonFixture> all = generator.generateAll();

        Set<PoisonType> produced = all.stream()
                .map(PoisonFixture::type)
                .collect(Collectors.toSet());

        assertEquals(EnumSet.allOf(PoisonType.class), produced,
                "generator must emit exactly one fixture per PoisonType");
        assertEquals(PoisonType.values().length, all.size(), "no duplicate or missing fixtures");
    }

    @Test
    void truncatedAvroIsConfluentFramedAndShorterThanAFullBody() {
        PoisonFixture f = generator.generate(PoisonType.TRUNCATED_AVRO);
        assertEquals(PoisonFixtureGenerator.CONFLUENT_MAGIC_BYTE, f.value()[0], "starts with magic byte");
        assertTrue(f.value().length > 5, "has a (partial) body after the 5-byte header");
    }

    @Test
    void jsonWrongTypeIsValidUtf8Json() {
        PoisonFixture f = generator.generate(PoisonType.JSON_WRONG_TYPE);
        String s = new String(f.value(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(s.startsWith("{") && s.endsWith("}"), "looks like a JSON object: " + s);
    }

    @Test
    void oversizedExceedsThreshold() {
        PoisonFixture f = generator.generate(PoisonType.OVERSIZED);
        assertTrue(f.value().length > PoisonFixtureGenerator.OVERSIZED_THRESHOLD_BYTES,
                "oversized fixture must exceed the threshold");
    }

    @Test
    void nullKeyValidHasNoKey() {
        PoisonFixture f = generator.generate(PoisonType.NULL_KEY_VALID);
        assertNull(f.key(), "control fixture must have a null key");
        assertNotNull(f.value());
    }

    @Test
    void generationIsDeterministic() {
        // Spec §3: same inputs -> byte-identical output.
        assertArrayEquals(
                generator.generate(PoisonType.SCHEMA_VERSION_MISMATCH).value(),
                generator.generate(PoisonType.SCHEMA_VERSION_MISMATCH).value(),
                "fixtures must be reproducible");
    }
}
