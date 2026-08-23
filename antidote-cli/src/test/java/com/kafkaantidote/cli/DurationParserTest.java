package com.kafkaantidote.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DurationParserTest {

    @Test
    void parsesMilliseconds() {
        assertEquals(Duration.ofMillis(300), DurationParser.parse("300ms"));
    }

    @Test
    void parsesSeconds() {
        assertEquals(Duration.ofSeconds(5), DurationParser.parse("5s"));
    }

    @Test
    void parsesMinutes() {
        assertEquals(Duration.ofMinutes(2), DurationParser.parse("2m"));
    }

    @Test
    void bareNumberIsSeconds() {
        assertEquals(Duration.ofSeconds(10), DurationParser.parse("10"));
    }

    @Test
    void toleratesWhitespace() {
        assertEquals(Duration.ofSeconds(5), DurationParser.parse("  5s "));
    }

    @Test
    void rejectsGarbage() {
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse("soon"));
    }

    @Test
    void rejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse(""));
    }
}
