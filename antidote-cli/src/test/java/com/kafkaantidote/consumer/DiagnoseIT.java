package com.kafkaantidote.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.cli.Antidote;
import com.kafkaantidote.cli.ExitCodes;
import com.kafkaantidote.core.GroupCoordinates;
import com.kafkaantidote.core.StuckPosition;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import picocli.CommandLine;

/**
 * The R1 contract against a real broker (Spec R1.1–R1.4, plus the diagnose command R5.1/R5.5).
 * Integration is the source of truth for this tool (Implementation Plan §2). Ground rule G1: a
 * disposable Testcontainers broker only.
 */
@Testcontainers
class DiagnoseIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    // Short sampling so tests stay fast; 2 samples => a single poll-interval sleep.
    private static final int SAMPLES = 2;
    private static final Duration INTERVAL = Duration.ofMillis(200);

    private ConsumerMessageSource newSource() {
        return new ConsumerMessageSource(SAMPLES, INTERVAL);
    }

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    // ---- R1.1 / R1.2: a genuinely stuck partition is detected with the right numbers ----
    @Test
    void detectsStuckPartitionWithCorrectNumbers() throws Exception {
        String topic = "stuck-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);          // offsets 0..4, log-end = 5
        setCommitted(group, topic, 0, 2); // group consumed 0,1; parked at 2 -> lag 3

        List<StuckPosition> stuck = newSource()
                .findStuckPositions(new GroupCoordinates(bootstrap(), group));

        assertEquals(1, stuck.size(), "exactly one stuck partition");
        StuckPosition p = stuck.get(0);
        assertEquals(topic, p.topic());
        assertEquals(0, p.partition());
        assertEquals(2, p.committedOffset());
        assertEquals(5, p.logEndOffset());
        assertEquals(3, p.lag());
    }

    // ---- R1.4: the detection path must not commit, seek, or mutate the group's offsets ----
    @Test
    void detectionDoesNotMutateOffsets() throws Exception {
        String topic = "readonly-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);
        setCommitted(group, topic, 0, 2);

        long before = readCommitted(group, topic, 0);
        newSource().findStuckPositions(new GroupCoordinates(bootstrap(), group));
        long after = readCommitted(group, topic, 0);

        assertEquals(before, after, "committed offset must be untouched by diagnosis (R1.4)");
        assertEquals(2, after);
    }

    // ---- R1.3: nothing stuck when the group is fully caught up ----
    @Test
    void noPoisonWhenCaughtUp() throws Exception {
        String topic = "clean-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);
        setCommitted(group, topic, 0, 5); // consumed everything, lag 0

        assertTrue(newSource().findStuckPositions(new GroupCoordinates(bootstrap(), group)).isEmpty(),
                "a caught-up group has no stuck partitions (R1.3)");
    }

    // ---- R1.1 across partitions: only the stuck one is reported ----
    @Test
    void reportsOnlyTheStuckPartition() throws Exception {
        String topic = "mixed-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 2);
        produce(topic, 0, 5);
        produce(topic, 1, 5);
        setCommitted(group, topic, 0, 5); // partition 0 healthy
        setCommitted(group, topic, 1, 1); // partition 1 stuck, lag 4

        List<StuckPosition> stuck = newSource()
                .findStuckPositions(new GroupCoordinates(bootstrap(), group));

        assertEquals(1, stuck.size());
        assertEquals(1, stuck.get(0).partition());
        assertEquals(4, stuck.get(0).lag());
    }

    // ---- R5.1 / R5.5: the diagnose command reports and returns meaningful exit codes ----
    @Test
    void diagnoseCommandExitsPoisonDetectedAndPrintsStuckPartition() throws Exception {
        String topic = "cmd-stuck-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);
        setCommitted(group, topic, 0, 2);

        StringWriter out = new StringWriter();
        int code = new CommandLine(new Antidote()).setOut(new PrintWriter(out)).execute(
                "diagnose", "--bootstrap", bootstrap(), "--group", group,
                "--poll-interval", "200ms", "--samples", "2");

        assertEquals(ExitCodes.POISON_DETECTED, code);
        assertTrue(out.toString().contains(topic), "report names the stuck topic: " + out);
    }

    @Test
    void diagnoseCommandExitsOkWhenNoPoison() throws Exception {
        String topic = "cmd-clean-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);
        setCommitted(group, topic, 0, 5);

        StringWriter out = new StringWriter();
        int code = new CommandLine(new Antidote()).setOut(new PrintWriter(out)).execute(
                "diagnose", "--bootstrap", bootstrap(), "--group", group,
                "--poll-interval", "200ms", "--samples", "2");

        assertEquals(ExitCodes.OK, code);
        assertTrue(out.toString().toLowerCase().contains("no poison"), "output: " + out);
    }

    // ------------------------------ test fixtures / helpers ------------------------------

    private void createTopic(String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private void produce(String topic, int partition, int count) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, partition, null,
                        ("m" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))).get();
            }
        }
    }

    /** Sets the group's committed offset via assign+commit (no membership, no rebalance). */
    private void setCommitted(String group, String topic, int partition, long offset) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            c.assign(List.of(tp));
            c.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
        }
    }

    private long readCommitted(String group, String topic, int partition) throws Exception {
        try (Admin admin = Admin.create(adminProps())) {
            Map<TopicPartition, OffsetAndMetadata> m =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata om = m.get(new TopicPartition(topic, partition));
            return om == null ? -1 : om.offset();
        }
    }

    private Properties adminProps() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        return p;
    }
}
