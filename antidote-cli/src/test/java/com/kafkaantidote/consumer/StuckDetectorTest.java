package com.kafkaantidote.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.core.StuckPosition;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure two-sample stuck decision (Spec R1.1). No broker, fully deterministic.
 */
class StuckDetectorTest {

    private static final TopicPartition TP0 = new TopicPartition("orders", 0);
    private static final TopicPartition TP1 = new TopicPartition("orders", 1);

    @Test
    void stationaryCommittedWithNonZeroLagIsStuck() {
        // committed frozen at 2 across both samples; lag 3 -> stuck (R1.1).
        var first = Map.of(TP0, new PartitionOffsets(2, 5));
        var last = Map.of(TP0, new PartitionOffsets(2, 5));

        List<StuckPosition> stuck = StuckDetector.detect(first, last);

        assertEquals(1, stuck.size());
        StuckPosition p = stuck.get(0);
        assertEquals("orders", p.topic());
        assertEquals(0, p.partition());
        assertEquals(2, p.committedOffset());
        assertEquals(5, p.logEndOffset());
        assertEquals(3, p.lag());
    }

    @Test
    void advancingCommittedIsNotStuck() {
        // committed moved 2 -> 4 between samples: merely slow, not stuck (R1.1 core distinction).
        var first = Map.of(TP0, new PartitionOffsets(2, 5));
        var last = Map.of(TP0, new PartitionOffsets(4, 5));

        assertTrue(StuckDetector.detect(first, last).isEmpty());
    }

    @Test
    void zeroLagIsNotStuck() {
        // caught up: committed == logEnd, lag 0 -> not stuck (feeds R1.3 "no poison").
        var first = Map.of(TP0, new PartitionOffsets(5, 5));
        var last = Map.of(TP0, new PartitionOffsets(5, 5));

        assertTrue(StuckDetector.detect(first, last).isEmpty());
    }

    @Test
    void stationaryCommittedWhileLogEndGrowsIsStuck() {
        // committed frozen, but new messages keep arriving (logEnd 5 -> 9): still stuck, lag grows.
        var first = Map.of(TP0, new PartitionOffsets(2, 5));
        var last = Map.of(TP0, new PartitionOffsets(2, 9));

        List<StuckPosition> stuck = StuckDetector.detect(first, last);
        assertEquals(1, stuck.size());
        assertEquals(7, stuck.get(0).lag());
    }

    @Test
    void onlyStuckPartitionsReported_andOrderedDeterministically() {
        // TP1 stuck, TP0 healthy -> only TP1; output ordered by topic then partition.
        var first = Map.of(
                TP0, new PartitionOffsets(5, 5),
                TP1, new PartitionOffsets(2, 8));
        var last = Map.of(
                TP0, new PartitionOffsets(5, 5),
                TP1, new PartitionOffsets(2, 8));

        List<StuckPosition> stuck = StuckDetector.detect(first, last);
        assertEquals(1, stuck.size());
        assertEquals(1, stuck.get(0).partition());
    }

    @Test
    void partitionMissingFromFirstSampleIsSkipped() {
        // Can't judge "stationary" without a baseline -> skip rather than guess.
        var first = Map.<TopicPartition, PartitionOffsets>of();
        var last = Map.of(TP0, new PartitionOffsets(2, 5));

        assertTrue(StuckDetector.detect(first, last).isEmpty());
    }
}
