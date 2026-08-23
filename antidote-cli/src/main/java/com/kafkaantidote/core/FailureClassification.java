package com.kafkaantidote.core;

import java.util.Objects;

/**
 * The best-effort verdict on why a message failed (Spec R3). Immutable value type (Spec §4.3).
 *
 * <p>Per R3.2 a classification is ALWAYS a heuristic — {@link #confidence} exists to make that
 * explicit in output; the tool must never assert a cause as certain.
 */
public record FailureClassification(Category category, String detail, Confidence confidence) {

    /** The four cause buckets required by R3.1. */
    public enum Category {
        DESERIALIZATION_FAILURE,
        SCHEMA_VERSION_MISMATCH,
        OVERSIZED_MESSAGE,
        UNKNOWN
    }

    /** How strongly the heuristic believes its own guess (never CERTAIN — see R3.2). */
    public enum Confidence {
        LIKELY,
        POSSIBLE,
        UNSURE
    }

    public FailureClassification {
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(detail, "detail");
        Objects.requireNonNull(confidence, "confidence");
    }

    public String toHuman() {
        return "[heuristic: " + confidence + "] " + category + " — " + detail;
    }

    public String toJson() {
        return "{\"category\":" + Json.quote(category.name())
                + ",\"confidence\":" + Json.quote(confidence.name())
                + ",\"heuristic\":true"
                + ",\"detail\":" + Json.quote(detail) + "}";
    }
}
