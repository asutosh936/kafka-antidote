package com.kafkaantidote.consumer;

import com.kafkaantidote.core.GroupCoordinates;
import com.kafkaantidote.core.MessageFetchException;
import com.kafkaantidote.core.MessageSource;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.ReinjectionPlan;
import com.kafkaantidote.core.StuckPosition;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * The v0.1 {@link MessageSource} implementation for plain Kafka consumers (Spec §4.2). It is the
 * ONLY class that talks to {@code kafka-clients}; everything above the {@code core} boundary depends
 * on the interface, not on this class.
 *
 * <p>{@code planReinjection} arrives in Phase 3.
 */
public final class ConsumerMessageSource implements MessageSource {

    private final String bootstrapServers;
    private final int samples;
    private final Duration pollInterval;

    /**
     * @param bootstrapServers the cluster this source talks to (used by {@code fetchRaw}, whose
     *                         {@link TopicPartitionOffset} carries no bootstrap of its own)
     * @param samples          how many offset samples must show the committed offset stationary
     *                         before a partition is reported stuck (Spec R1.1); must be >= 1
     * @param pollInterval     the gap between samples
     */
    public ConsumerMessageSource(String bootstrapServers, int samples, Duration pollInterval) {
        if (samples < 1) {
            throw new IllegalArgumentException("samples must be >= 1, was " + samples);
        }
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
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

    /** How long to wait for the record at the requested offset before giving up. */
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(15);

    @Override
    public RawMessage fetchRaw(TopicPartitionOffset position) {
        Objects.requireNonNull(position, "position");
        TopicPartition tp = new TopicPartition(position.topic(), position.partition());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(fetchProps())) {
            // assign + seek only — never subscribe/commit, so the target group is untouched (read-only).
            consumer.assign(List.of(tp));

            long begin = consumer.beginningOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
            if (position.offset() < begin || position.offset() >= end) {
                throw new MessageFetchException("No message at " + position.toHuman()
                        + " — " + position.topic() + "-" + position.partition()
                        + (end == begin ? " is empty."
                        : " holds offsets " + begin + ".." + (end - 1) + ".")
                        + " Check the topic/partition/offset, or run `diagnose` first.");
            }

            consumer.seek(tp, position.offset());
            long deadline = System.currentTimeMillis() + FETCH_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                    if (record.offset() == position.offset()) {
                        return toRawMessage(position, record);
                    }
                    if (record.offset() > position.offset()) {
                        throw new MessageFetchException("Offset " + position.offset() + " is not present at "
                                + position.topic() + "-" + position.partition()
                                + " (it may have been compacted or deleted); the next available offset is "
                                + record.offset() + ".");
                    }
                }
            }
            throw new MessageFetchException("Timed out after " + FETCH_TIMEOUT.toSeconds()
                    + "s fetching " + position.toHuman() + ". Is the broker reachable?");
        } catch (KafkaException e) {
            throw new MessageFetchException("Could not fetch " + position.toHuman()
                    + ": " + rootMessage(e), e);
        }
    }

    private static RawMessage toRawMessage(TopicPartitionOffset position,
                                           ConsumerRecord<byte[], byte[]> record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            headers.put(h.key(), h.value() == null ? new byte[0] : h.value());
        }
        byte[] value = record.value() == null ? new byte[0] : record.value();
        int size = record.serializedValueSize() >= 0 ? record.serializedValueSize() : value.length;
        return new RawMessage(position, value, record.key(), headers, record.timestamp(), size);
    }

    private Properties fetchProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // ByteArrayDeserializer bypasses the failing deserializer entirely (R2.1).
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // A throwaway group that never commits — assign()-based access does not join or affect it.
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "antidote-inspector-" + System.nanoTime());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        return p;
    }

    @Override
    public ReinjectionPlan planReinjection(RawMessage original, byte[] correctedPayload) {
        throw new UnsupportedOperationException("planReinjection is implemented in Phase 3");
    }
}
