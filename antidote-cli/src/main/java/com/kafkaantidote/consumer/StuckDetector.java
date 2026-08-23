package com.kafkaantidote.consumer;

import com.kafkaantidote.core.StuckPosition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/**
 * The pure two-sample "is it stuck?" decision (Spec R1.1), separated from all Kafka I/O so it can be
 * unit-tested deterministically.
 *
 * <p>A partition is stuck when, across the sampling window, its committed offset did not advance
 * while its lag remained non-zero. A partition absent from the first sample has no baseline and is
 * skipped rather than guessed at.
 */
final class StuckDetector {

    private StuckDetector() {
    }

    /**
     * Decide which partitions are stuck given the first and last offset samples.
     *
     * @param first offsets at the start of the sampling window
     * @param last  offsets at the end of the sampling window
     * @return the stuck partitions, ordered by topic then partition for determinism (Spec §3)
     */
    static List<StuckPosition> detect(Map<TopicPartition, PartitionOffsets> first,
                                      Map<TopicPartition, PartitionOffsets> last) {
        List<StuckPosition> stuck = new ArrayList<>();
        for (Map.Entry<TopicPartition, PartitionOffsets> entry : last.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionOffsets now = entry.getValue();
            PartitionOffsets baseline = first.get(tp);
            if (baseline == null) {
                continue; // no baseline to judge "stationary" against
            }
            boolean committedStationary = baseline.committed() == now.committed();
            boolean lagNonZero = now.lag() > 0;
            if (committedStationary && lagNonZero) {
                stuck.add(new StuckPosition(
                        tp.topic(), tp.partition(), now.committed(), now.logEnd(), now.lag()));
            }
        }
        stuck.sort(Comparator.comparing(StuckPosition::topic).thenComparingInt(StuckPosition::partition));
        return List.copyOf(stuck);
    }
}
