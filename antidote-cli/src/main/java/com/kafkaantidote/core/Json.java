package com.kafkaantidote.core;

/**
 * Minimal JSON string helpers so the value types can render a {@code --json} form (Spec §4.3, R5.6)
 * without a JSON-library dependency. Not a general JSON framework — just enough to emit the tool's
 * small, fixed output shapes correctly and safely.
 */
public final class Json {

    private Json() {
    }

    /** @return a JSON string literal (quoted, escaped), or the literal {@code null} for a null input. */
    public static String quote(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder b = new StringBuilder(s.length() + 2);
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                case '\b' -> b.append("\\b");
                case '\f' -> b.append("\\f");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        b.append('"');
        return b.toString();
    }
}
