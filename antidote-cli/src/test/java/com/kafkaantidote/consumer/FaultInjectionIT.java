package com.kafkaantidote.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * Fault-injection hardening (Spec R6, Implementation Plan Phase 4): behavior when things are already
 * broken — a second poison pill stacked behind the first, and an unreachable broker. The tool must
 * stay correct and fail cleanly (no stack trace as primary output).
 */
@Testcontainers
class FaultInjectionIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    private static final String UNREACHABLE = "127.0.0.1:1"; // nothing listens here

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    // ---- R6/R1: a second poison pill behind the first is found once the first is dealt with ----
    @Test
    void secondPoisonPillEmergesAfterTheFirstIsAdvancedPast() throws Exception {
        String topic = "stacked-" + System.nanoTime();
        String group = "g-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, 5);              // offsets 0..4
        setCommitted(group, topic, 0, 0);  // stuck on the FIRST pill at offset 0

        ConsumerMessageSource source = new ConsumerMessageSource(bootstrap(), 2, Duration.ofMillis(200));

        List<StuckPosition> firstJam = source.findStuckPositions(new GroupCoordinates(bootstrap(), group));
        assertEquals(1, firstJam.size());
        assertEquals(0, firstJam.get(0).committedOffset(), "stuck on the first pill");

        // Operator fixes/skip the first pill -> committed advances to the SECOND pill at offset 1.
        setCommitted(group, topic, 0, 1);

        List<StuckPosition> secondJam = source.findStuckPositions(new GroupCoordinates(bootstrap(), group));
        assertEquals(1, secondJam.size());
        assertEquals(1, secondJam.get(0).committedOffset(), "the second pill is now surfaced");
        assertEquals(4, secondJam.get(0).lag());
    }

    // ---- R2.4/R6: unreachable broker -> inspect fails cleanly, no stack trace ----
    @Test
    void inspectAgainstUnreachableBrokerFailsCleanly() {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int code = new CommandLine(new Antidote())
                .setOut(new PrintWriter(out)).setErr(new PrintWriter(err))
                .execute("inspect", "--bootstrap", UNREACHABLE, "--topic", "x", "--partition", "0", "--offset", "0");

        assertEquals(ExitCodes.FETCH_FAILED, code);
        assertNoStackTrace(err.toString());
    }

    // ---- R6: unreachable broker -> diagnose fails cleanly with the connection exit code ----
    @Test
    void diagnoseAgainstUnreachableBrokerFailsCleanly() {
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int code = new CommandLine(new Antidote())
                .setOut(new PrintWriter(out)).setErr(new PrintWriter(err))
                .execute("diagnose", "--bootstrap", UNREACHABLE, "--group", "g",
                        "--poll-interval", "100ms", "--samples", "2");

        assertEquals(ExitCodes.CONNECTION, code);
        assertNoStackTrace(err.toString());
    }

    private static void assertNoStackTrace(String errText) {
        assertFalse(errText.contains("at com.kafkaantidote"), "no stack trace in output: " + errText);
        assertFalse(errText.contains("\tat "), "no stack frames in output: " + errText);
        assertTrue(errText.strip().length() > 0, "there is an actionable message");
    }

    // ------------------------------ helpers ------------------------------

    private void createTopic(String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        try (Admin admin = Admin.create(p)) {
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
}
