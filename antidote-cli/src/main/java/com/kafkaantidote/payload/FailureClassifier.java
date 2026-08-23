package com.kafkaantidote.payload;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.FailureClassification.Category;
import com.kafkaantidote.core.FailureClassification.Confidence;
import com.kafkaantidote.core.RawMessage;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Best-effort heuristic classifier for a fetched payload (Spec R3). Depends only on the {@code core}
 * value types — never on Kafka classes.
 *
 * <p>Every verdict is a labeled heuristic (R3.2): the {@link Confidence} is never "certain", and the
 * detail text always states what was observed, not what is proven. Schema detection is wire-format
 * only — no Schema Registry is contacted (R3.3).
 */
public final class FailureClassifier {

    /** Payloads at/above this are flagged as potentially oversized (~1 MiB). */
    static final int OVERSIZED_THRESHOLD_BYTES = 1024 * 1024;

    /** Confluent Schema Registry wire-format magic byte. */
    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;

    private FailureClassifier() {
    }

    public static FailureClassification classify(RawMessage message) {
        byte[] value = message.value();

        if (value.length > OVERSIZED_THRESHOLD_BYTES) {
            return new FailureClassification(Category.OVERSIZED_MESSAGE,
                    "payload is " + value.length + " bytes (> ~1 MiB); it may exceed the broker's or "
                            + "consumer's max message size",
                    Confidence.POSSIBLE);
        }

        if (isConfluentFramed(value)) {
            int schemaId = readInt(value, 1);
            return new FailureClassification(Category.SCHEMA_VERSION_MISMATCH,
                    "Confluent wire-format detected (magic byte 0x00, schema id " + schemaId + "); the "
                            + "consumer may expect a different registered schema version — this could also "
                            + "be a truncated or corrupt Avro body (wire-format heuristic only, no registry "
                            + "was contacted)",
                    Confidence.POSSIBLE);
        }

        String utf8 = decodeStrictUtf8(value);
        if (utf8 != null) {
            String trimmed = utf8.strip();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                return new FailureClassification(Category.UNKNOWN,
                        "decodes cleanly as UTF-8 and looks like JSON; no framing corruption is visible — "
                                + "if deserialization still fails, the consumer likely expects a different "
                                + "type or schema than this JSON provides",
                        Confidence.UNSURE);
            }
            return new FailureClassification(Category.DESERIALIZATION_FAILURE,
                    "decodes as UTF-8 text but is not a recognized structured format; the configured "
                            + "deserializer may reject it",
                    Confidence.UNSURE);
        }

        return new FailureClassification(Category.DESERIALIZATION_FAILURE,
                "not valid UTF-8 and no known framing (e.g. Confluent magic byte) was found; likely a "
                        + "binary payload the configured deserializer cannot parse",
                Confidence.UNSURE);
    }

    private static boolean isConfluentFramed(byte[] value) {
        return value.length >= 5 && value[0] == CONFLUENT_MAGIC_BYTE;
    }

    private static int readInt(byte[] b, int offset) {
        return ((b[offset] & 0xFF) << 24)
                | ((b[offset + 1] & 0xFF) << 16)
                | ((b[offset + 2] & 0xFF) << 8)
                | (b[offset + 3] & 0xFF);
    }

    /** @return the decoded string if {@code value} is valid UTF-8, otherwise {@code null}. */
    private static String decodeStrictUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
