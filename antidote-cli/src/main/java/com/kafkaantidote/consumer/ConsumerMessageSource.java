package com.kafkaantidote.consumer;

import com.kafkaantidote.core.GroupCoordinates;
import com.kafkaantidote.core.MessageSource;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.ReinjectionPlan;
import com.kafkaantidote.core.StuckPosition;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * The v0.1 {@link MessageSource} implementation for plain Kafka consumers (Spec §4.2). It is the
 * ONLY class that talks to {@code kafka-clients}; everything above the {@code core} boundary depends
 * on the interface, not on this class.
 *
 * <p>Skeleton — {@link #findStuckPositions} is implemented in the GREEN step of Phase 1;
 * {@code fetchRaw} and {@code planReinjection} arrive in Phases 2 and 3.
 */
public final class ConsumerMessageSource implements MessageSource {

    private final int samples;
    private final Duration pollInterval;

    /**
     * @param samples      how many offset samples must show the committed offset stationary before a
     *                     partition is reported stuck (Spec R1.1); must be >= 1
     * @param pollInterval the gap between samples
     */
    public ConsumerMessageSource(int samples, Duration pollInterval) {
        if (samples < 1) {
            throw new IllegalArgumentException("samples must be >= 1, was " + samples);
        }
        this.samples = samples;
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
    }

    @Override
    public List<StuckPosition> findStuckPositions(GroupCoordinates group) {
        Objects.requireNonNull(group, "group");
        try (Admin admin = Admin.create(adminProps(group.bootstrapServers()))) {
            // Take `samples` snapshots separated by pollInterval; compare first with last (R1.1).
            Map<TopicPartition, PartitionOffsets> first = sample(admin, group.groupId());
            Map<TopicPartition, PartitionOffsets> last = first;
            for (int i = 1; i < samples; i++) {
                sleep(pollInterval);
                last = sample(admin, group.groupId());
            }
            return StuckDetector.detect(first, last);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while sampling offsets for group "
                    + group.groupId(), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("could not read offsets for group " + group.groupId()
                    + ": " + rootMessage(e), e);
        }
    }

    /**
     * One read-only snapshot: the group's committed offsets and the current log-end offset per
     * partition. Uses only AdminClient reads — no commit, no seek, no group membership (R1.4).
     */
    private Map<TopicPartition, PartitionOffsets> sample(Admin admin, String groupId)
            throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        if (committed.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : committed.keySet()) {
            latestSpecs.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResultInfo> logEnds = admin.listOffsets(latestSpecs).all().get();

        Map<TopicPartition, PartitionOffsets> snapshot = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committedOffset = entry.getValue().offset();
            ListOffsetsResultInfo end = logEnds.get(tp);
            if (end == null) {
                continue;
            }
            snapshot.put(tp, new PartitionOffsets(committedOffset, end.offset()));
        }
        return snapshot;
    }

    private static void sleep(Duration d) throws InterruptedException {
        long millis = d.toMillis();
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    private static Properties adminProps(String bootstrapServers) {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return p;
    }

    @Override
    public RawMessage fetchRaw(TopicPartitionOffset position) {
        throw new UnsupportedOperationException("fetchRaw is implemented in Phase 2");
    }

    @Override
    public ReinjectionPlan planReinjection(RawMessage original, byte[] correctedPayload) {
        throw new UnsupportedOperationException("planReinjection is implemented in Phase 3");
    }
}
