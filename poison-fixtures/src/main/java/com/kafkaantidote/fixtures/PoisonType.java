package com.kafkaantidote.fixtures;

/**
 * The five poison-pill categories the v0.1 corpus must cover (Implementation Plan, Phase 0).
 *
 * <p>NULL_KEY_VALID is the control: a perfectly valid message with a null key. It is included so
 * detection/classification logic can be proven NOT to raise false positives on healthy data.
 */
public enum PoisonType {
    /** Confluent-framed Avro whose body is cut short mid-record. */
    TRUNCATED_AVRO,
    /** Syntactically valid JSON, but a field holds the wrong type (e.g. string where a number is expected). */
    JSON_WRONG_TYPE,
    /** Confluent-framed payload referencing a schema id the consumer cannot resolve to its expected version. */
    SCHEMA_VERSION_MISMATCH,
    /** A payload far larger than a typical message, standing in for an oversized record. */
    OVERSIZED,
    /** Control: a valid message with a null key (NOT actually poison). */
    NULL_KEY_VALID
}
