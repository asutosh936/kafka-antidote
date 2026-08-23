package com.kafkaantidote.cli;

import java.time.Duration;

/**
 * Parses human-friendly durations for CLI options like {@code --poll-interval} (Spec R5.1):
 * {@code "300ms"}, {@code "5s"}, {@code "2m"}, or a bare number treated as seconds.
 *
 * <p>Skeleton — implemented in the GREEN step of Phase 1.
 */
final class DurationParser {

    private DurationParser() {
    }

    static Duration parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        String s = text.trim();
        if (s.isEmpty()) {
            throw new IllegalArgumentException("duration must not be empty");
        }
        try {
            if (s.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2).trim()));
            }
            if (s.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1).trim()));
            }
            if (s.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1).trim()));
            }
            return Duration.ofSeconds(Long.parseLong(s)); // bare number = seconds
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid duration '" + text + "' — use e.g. 300ms, 5s, 2m", e);
        }
    }
}
