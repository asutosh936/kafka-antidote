package com.kafkaantidote.fixtures;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Generates the v0.1 poison-pill corpus (Implementation Plan, Phase 0).
 *
 * <p>Deterministic by design (Spec §3 "Determinism"): the same call always yields byte-identical
 * fixtures, so tests are reproducible. No randomness, no Kafka, no Avro/JSON libraries — the bytes
 * are hand-crafted so the dependency surface stays minimal.
 *
 * <p>The Confluent wire format used by two of the fixtures is: one magic byte {@code 0x00},
 * then a 4-byte big-endian schema id, then the serialized body.
 */
public final class PoisonFixtureGenerator {

    /** Confluent Schema Registry wire-format magic byte. */
    public static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    /** Anything at or above this size is treated as "oversized" by the corpus (1 MiB). */
    public static final int OVERSIZED_THRESHOLD_BYTES = 1024 * 1024;

    /** @return one fixture per {@link PoisonType}, in enum order. */
    public List<PoisonFixture> generateAll() {
        Map<PoisonType, PoisonFixture> byType = new EnumMap<>(PoisonType.class);
        for (PoisonType type : PoisonType.values()) {
            byType.put(type, generate(type));
        }
        return List.copyOf(byType.values());
    }

    public PoisonFixture generate(PoisonType type) {
        return switch (type) {
            case TRUNCATED_AVRO -> truncatedAvro();
            case JSON_WRONG_TYPE -> jsonWrongType();
            case SCHEMA_VERSION_MISMATCH -> schemaVersionMismatch();
            case OVERSIZED -> oversized();
            case NULL_KEY_VALID -> nullKeyValid();
        };
    }

    private PoisonFixture truncatedAvro() {
        // A full Confluent-framed record would carry a complete Avro body; here the body is cut off
        // mid-field so a real Avro reader would hit EOF partway through decoding.
        byte[] fullBody = new byte[] {
                0x0C, 'o', 'r', 'd', 'e', 'r', '-', '1',   // string field: length-prefixed "order-1"
                (byte) 0x96, 0x01,                          // varint amount
                0x02                                        // start of another field...
        };
        int truncateTo = fullBody.length / 2; // chop it in half
        byte[] truncatedBody = new byte[truncateTo];
        System.arraycopy(fullBody, 0, truncatedBody, 0, truncateTo);
        byte[] value = confluentFrame(101, truncatedBody);
        return new PoisonFixture(
                PoisonType.TRUNCATED_AVRO,
                "order-1".getBytes(StandardCharsets.UTF_8),
                value,
                Map.of("content-type", "application/avro".getBytes(StandardCharsets.UTF_8)),
                "Confluent-framed Avro (schema id 101) truncated to " + truncateTo + " body bytes");
    }

    private PoisonFixture jsonWrongType() {
        // Valid JSON, but "amount" is a boolean and "quantity" is a string where numbers are expected.
        String json = "{\"id\":\"order-2\",\"amount\":true,\"quantity\":\"seven\"}";
        return new PoisonFixture(
                PoisonType.JSON_WRONG_TYPE,
                "order-2".getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8),
                Map.of("content-type", "application/json".getBytes(StandardCharsets.UTF_8)),
                "Syntactically valid JSON with wrong field types (amount=boolean, quantity=string)");
    }

    private PoisonFixture schemaVersionMismatch() {
        // Confluent-framed, but referencing a schema id the consumer is not expected to resolve to
        // its current version. Body is well-formed for SOME schema, just not the reader's.
        byte[] body = new byte[] {
                0x0C, 'o', 'r', 'd', 'e', 'r', '-', '3',   // "order-3"
                (byte) 0xC8, 0x01,                          // varint field the new schema dropped
                0x02, 'X'                                    // an extra field the reader doesn't know
        };
        byte[] value = confluentFrame(99999, body); // implausibly high id -> not the reader's version
        return new PoisonFixture(
                PoisonType.SCHEMA_VERSION_MISMATCH,
                "order-3".getBytes(StandardCharsets.UTF_8),
                value,
                Map.of("content-type", "application/avro".getBytes(StandardCharsets.UTF_8)),
                "Confluent-framed with unresolved schema id 99999 (version mismatch)");
    }

    private PoisonFixture oversized() {
        int size = OVERSIZED_THRESHOLD_BYTES + 1;
        byte[] value = new byte[size];
        // Fill deterministically so the fixture is byte-stable across runs.
        for (int i = 0; i < size; i++) {
            value[i] = (byte) (i & 0xFF);
        }
        return new PoisonFixture(
                PoisonType.OVERSIZED,
                "order-4".getBytes(StandardCharsets.UTF_8),
                value,
                Map.of("content-type", "application/octet-stream".getBytes(StandardCharsets.UTF_8)),
                "Oversized payload of " + size + " bytes (> " + OVERSIZED_THRESHOLD_BYTES + ")");
    }

    private PoisonFixture nullKeyValid() {
        // The control: valid JSON, null key. Not poison; used to prove no false positives.
        String json = "{\"id\":\"order-5\",\"amount\":42,\"quantity\":7}";
        return new PoisonFixture(
                PoisonType.NULL_KEY_VALID,
                null,
                json.getBytes(StandardCharsets.UTF_8),
                Map.of("content-type", "application/json".getBytes(StandardCharsets.UTF_8)),
                "Valid JSON message with a null key (control, not poison)");
    }

    /** Wraps {@code body} in the Confluent wire format: magic byte + 4-byte big-endian schema id + body. */
    static byte[] confluentFrame(int schemaId, byte[] body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(5 + body.length);
        out.write(CONFLUENT_MAGIC_BYTE);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        out.write(body, 0, body.length);
        return out.toByteArray();
    }
}
