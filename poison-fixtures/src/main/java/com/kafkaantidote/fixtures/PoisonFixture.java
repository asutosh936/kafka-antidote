package com.kafkaantidote.fixtures;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * One generated poison-pill message: the raw key/value bytes plus headers and the type it represents.
 *
 * <p>Immutable. Deliberately Kafka-agnostic — it is just data. Callers (tests, the tool's harness)
 * decide how to produce it to a broker.
 */
public final class PoisonFixture {

    private final PoisonType type;
    private final byte[] key;      // may be null (see NULL_KEY_VALID)
    private final byte[] value;
    private final Map<String, byte[]> headers;
    private final String description;

    public PoisonFixture(PoisonType type, byte[] key, byte[] value,
                         Map<String, byte[]> headers, String description) {
        this.type = Objects.requireNonNull(type, "type");
        this.key = key == null ? null : key.clone();
        this.value = Objects.requireNonNull(value, "value").clone();
        this.headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
        this.description = Objects.requireNonNull(description, "description");
    }

    public PoisonType type() {
        return type;
    }

    /** @return a defensive copy of the key, or {@code null} when the fixture has no key. */
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    /** @return a defensive copy of the value bytes. */
    public byte[] value() {
        return value.clone();
    }

    public Map<String, byte[]> headers() {
        return headers;
    }

    public String description() {
        return description;
    }

    @Override
    public String toString() {
        return "PoisonFixture{type=" + type
                + ", keyLen=" + (key == null ? "null" : key.length)
                + ", valueLen=" + value.length
                + ", headers=" + headers.keySet()
                + ", description='" + description + "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PoisonFixture that)) return false;
        return type == that.type
                && Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && headers.equals(that.headers)
                && description.equals(that.description);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, headers, description);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
