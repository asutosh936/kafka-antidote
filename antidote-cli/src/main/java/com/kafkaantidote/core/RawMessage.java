package com.kafkaantidote.core;

import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * The raw bytes at an offset, fetched WITHOUT the failing deserializer (Spec R2.1), plus the
 * metadata R2.3 requires: key, headers, timestamp, serialized size.
 *
 * <p>Immutable value type (Spec §4.3). Implemented as a final class rather than a record so the
 * byte-array fields can be defensively copied — a record would leak mutable internals.
 */
public final class RawMessage {

    private final TopicPartitionOffset position;
    private final byte[] value;
    private final byte[] key;                 // may be null
    private final Map<String, byte[]> headers;
    private final long timestamp;
    private final int serializedSize;

    public RawMessage(TopicPartitionOffset position, byte[] value, byte[] key,
                      Map<String, byte[]> headers, long timestamp, int serializedSize) {
        this.position = Objects.requireNonNull(position, "position");
        this.value = Objects.requireNonNull(value, "value").clone();
        this.key = key == null ? null : key.clone();
        this.headers = Map.copyOf(Objects.requireNonNull(headers, "headers"));
        this.timestamp = timestamp;
        this.serializedSize = serializedSize;
    }

    public TopicPartitionOffset position() {
        return position;
    }

    public byte[] value() {
        return value.clone();
    }

    public byte[] key() {
        return key == null ? null : key.clone();
    }

    public Map<String, byte[]> headers() {
        return headers;
    }

    public long timestamp() {
        return timestamp;
    }

    public int serializedSize() {
        return serializedSize;
    }

    public String toHuman() {
        return "message at " + position.toHuman()
                + " (" + serializedSize + " bytes, "
                + (key == null ? "null key" : key.length + "-byte key") + ", "
                + headers.size() + " headers, ts=" + timestamp + ")";
    }

    /** Machine-precise form: byte fields are base64-encoded so no bytes are lost. */
    public String toJson() {
        Base64.Encoder b64 = Base64.getEncoder();
        StringBuilder h = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, byte[]> e : headers.entrySet()) {
            if (!first) {
                h.append(',');
            }
            h.append(Json.quote(e.getKey())).append(':').append(Json.quote(b64.encodeToString(e.getValue())));
            first = false;
        }
        h.append('}');
        return "{\"position\":" + position.toJson()
                + ",\"keyBase64\":" + (key == null ? "null" : Json.quote(b64.encodeToString(key)))
                + ",\"valueBase64\":" + Json.quote(b64.encodeToString(value))
                + ",\"headers\":" + h
                + ",\"timestamp\":" + timestamp
                + ",\"serializedSize\":" + serializedSize + "}";
    }
}
