package com.kafkaantidote.payload;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.core.FailureClassification;
import com.kafkaantidote.core.FailureClassification.Category;
import com.kafkaantidote.core.FailureClassification.Confidence;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Presenter output (Spec R2.2, R2.3): three forms (hex, best-effort UTF-8, classification) plus
 * metadata, readable and truncation-safe.
 */
class PayloadPresenterTest {

    private static final FailureClassification CLASS =
            new FailureClassification(Category.UNKNOWN, "just a test", Confidence.UNSURE);

    private RawMessage message(byte[] value, byte[] key, Map<String, byte[]> headers, long ts, int size) {
        return new RawMessage(new TopicPartitionOffset("orders", 3, 84213), value, key, headers, ts, size);
    }

    @Test
    void showsAllThreeFormsAndMetadata() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        String out = PayloadPresenter.toHuman(
                message(value, "k1".getBytes(StandardCharsets.UTF_8),
                        Map.of("content-type", "text/plain".getBytes(StandardCharsets.UTF_8)),
                        1_700_000_000_000L, value.length),
                CLASS);

        assertTrue(out.contains("Message at orders-3@84213"), out);
        assertTrue(out.contains("[heuristic:"), "classification labeled as heuristic");
        assertTrue(out.contains("Raw payload (hex"), "hex section present");
        assertTrue(out.contains("Best-effort UTF-8"), "utf-8 section present");
        assertTrue(out.contains("68 65 6c 6c 6f"), "hex bytes of 'hello': " + out);
        assertTrue(out.contains("|hello|"), "ascii gutter shows hello");
        assertTrue(out.contains("content-type=text/plain"), "headers rendered");
        assertTrue(out.contains("size      : 5 bytes"), "size rendered");
    }

    @Test
    void rendersNullKeyExplicitly() {
        byte[] value = "x".getBytes(StandardCharsets.UTF_8);
        String out = PayloadPresenter.toHuman(message(value, null, Map.of(), 1L, 1), CLASS);
        assertTrue(out.contains("key       : <null>"), out);
        assertTrue(out.contains("headers   : (none)"), out);
    }

    @Test
    void truncatesLargePayloadsInTheHexView() {
        byte[] big = new byte[PayloadPresenter.MAX_DISPLAY_BYTES + 100];
        String out = PayloadPresenter.toHuman(message(big, null, Map.of(), 1L, big.length), CLASS);
        assertTrue(out.contains("first " + PayloadPresenter.MAX_DISPLAY_BYTES + " of " + big.length + " bytes"),
                "should note truncation: " + out.substring(0, Math.min(400, out.length())));
    }

    @Test
    void nonPrintableBytesRenderAsDots() {
        byte[] value = {0x00, 0x01, 'A', 0x7f};
        String out = PayloadPresenter.toHuman(message(value, null, Map.of(), 1L, value.length), CLASS);
        assertTrue(out.contains("..A."), "non-printable bytes shown as dots: " + out);
    }
}
