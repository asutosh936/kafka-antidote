package com.kafkaantidote.payload;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.RawMessage;
import java.time.Instant;
import java.util.Map;

/**
 * Renders a fetched payload in the three forms R2.2 requires — raw hex, best-effort UTF-8, and the
 * detected/attempted format (via the classifier) — plus the R2.3 metadata. Depends only on the
 * {@code core} value types.
 *
 * <p>Output is for a tired human at 2am (Spec §3): plain, scannable, no stack-trace noise. Long
 * payloads are truncated in the hex/UTF-8 views so the terminal stays readable.
 */
public final class PayloadPresenter {

    /** How many payload bytes to render in the hex / UTF-8 views before truncating. */
    static final int MAX_DISPLAY_BYTES = 256;

    private PayloadPresenter() {
    }

    public static String toHuman(RawMessage message, FailureClassification classification) {
        byte[] value = message.value();
        StringBuilder sb = new StringBuilder();

        sb.append("Message at ").append(message.position().toHuman()).append('\n');

        sb.append('\n').append("  Classification (heuristic)\n");
        sb.append("    ").append(classification.toHuman()).append('\n');

        sb.append('\n').append("  Metadata\n");
        sb.append("    key       : ").append(renderKey(message.key())).append('\n');
        sb.append("    headers   : ").append(renderHeaders(message.headers())).append('\n');
        sb.append("    timestamp : ").append(renderTimestamp(message.timestamp())).append('\n');
        sb.append("    size      : ").append(message.serializedSize()).append(" bytes\n");

        int shown = Math.min(value.length, MAX_DISPLAY_BYTES);
        String span = shown < value.length
                ? "first " + shown + " of " + value.length + " bytes"
                : value.length + " bytes";

        sb.append('\n').append("  Raw payload (hex, ").append(span).append(")\n");
        sb.append(hexDump(value, shown));

        sb.append('\n').append("  Best-effort UTF-8 (").append(span).append(")\n");
        sb.append("    ").append(printable(value, shown)).append('\n');

        return sb.toString();
    }

    private static String renderKey(byte[] key) {
        if (key == null) {
            return "<null>";
        }
        return printable(key, key.length) + "  (" + key.length + " bytes)";
    }

    private static String renderHeaders(Map<String, byte[]> headers) {
        if (headers.isEmpty()) {
            return "(none)";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, byte[]> e : headers.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append('=').append(printable(e.getValue(), e.getValue().length));
            first = false;
        }
        return sb.toString();
    }

    private static String renderTimestamp(long epochMillis) {
        if (epochMillis < 0) {
            return "n/a";
        }
        return Instant.ofEpochMilli(epochMillis) + " (epoch ms " + epochMillis + ")";
    }

    /** Classic hex dump: {@code offset  16 hex bytes  |ascii|}, 2-space indented. */
    static String hexDump(byte[] data, int limit) {
        if (limit == 0) {
            return "    (empty)\n";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < limit; i += 16) {
            sb.append("    ").append(String.format("%08x  ", i));
            StringBuilder ascii = new StringBuilder();
            for (int j = 0; j < 16; j++) {
                if (i + j < limit) {
                    int b = data[i + j] & 0xFF;
                    sb.append(String.format("%02x ", b));
                    ascii.append(b >= 0x20 && b < 0x7f ? (char) b : '.');
                } else {
                    sb.append("   ");
                }
                if (j == 7) {
                    sb.append(' ');
                }
            }
            sb.append(" |").append(ascii).append("|\n");
        }
        return sb.toString();
    }

    /** Best-effort printable rendering: printable ASCII kept, everything else shown as '.'. */
    static String printable(byte[] data, int limit) {
        StringBuilder sb = new StringBuilder(limit);
        for (int i = 0; i < limit; i++) {
            int b = data[i] & 0xFF;
            sb.append(b >= 0x20 && b < 0x7f ? (char) b : '.');
        }
        return sb.toString();
    }
}
