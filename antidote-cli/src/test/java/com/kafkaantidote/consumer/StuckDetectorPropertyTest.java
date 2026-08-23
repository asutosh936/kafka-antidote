package com.kafkaantidote.consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.core.StuckPosition;
import java.util.List;
import java.util.Map;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.apache.kafka.common.TopicPartition;

/**
 * Property-based hardening of the stuck decision (Phase 4). For any offsets, the detector must never
 * throw, and anything it flags as stuck must genuinely satisfy the R1.1 rule: committed offset
 * stationary across the window AND non-zero lag.
 */
class StuckDetectorPropertyTest {

    private static final TopicPartition TP = new TopicPartition("t", 0);

    @Property(tries = 2000)
    void anythingFlaggedStuckReallyIsStuck(
            @ForAll @LongRange(min = 0, max = 1_000_000) long committedFirst,
            @ForAll @LongRange(min = 0, max = 1_000_000) long committedLast,
            @ForAll @LongRange(min = 0, max = 1_000_000) long logEnd) {

        Map<TopicPartition, PartitionOffsets> first = Map.of(TP, new PartitionOffsets(committedFirst, logEnd));
        Map<TopicPartition, PartitionOffsets> last = Map.of(TP, new PartitionOffsets(committedLast, logEnd));

        List<StuckPosition> stuck = StuckDetector.detect(first, last); // must never throw

        for (StuckPosition p : stuck) {
            assertTrue(committedFirst == committedLast, "flagged stuck only if committed did not move");
            assertTrue(p.lag() > 0, "flagged stuck only if lag is non-zero");
        }
    }
}
