package com.kafkaantidote.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.cli.Antidote;
import com.kafkaantidote.cli.ExitCodes;
import com.kafkaantidote.core.MessageFetchException;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.TopicPartitionOffset;
import com.kafkaantidote.fixtures.PoisonFixture;
import com.kafkaantidote.fixtures.PoisonFixtureGenerator;
import com.kafkaantidote.fixtures.PoisonType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import picocli.CommandLine;

/**
 * The R2/R3 contract against a real broker: fetch raw bytes bypassing the deserializer, surface
 * metadata, classify, and fail cleanly on a missing offset. Ground rule G1: disposable broker only.
 */
@Testcontainers
class InspectIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    private final PoisonFixtureGenerator generator = new PoisonFixtureGenerator();

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    private ConsumerMessageSource newSource() {
        return new ConsumerMessageSource(bootstrap(), 1, Duration.ZERO);
    }

    // ---- R2.1 / R2.3: raw bytes are returned unchanged, with metadata surfaced ----
    @Test
    void fetchRawReturnsExactBytesKeyHeadersAndSize() throws Exception {
        String topic = "inspect-" + System.nanoTime();
        createTopic(topic, 1);
        PoisonFixture fixture = generator.generate(PoisonType.JSON_WRONG_TYPE); // has key + a header
        produce(topic, 0, fixture);

        RawMessage raw = newSource().fetchRaw(new TopicPartitionOffset(topic, 0, 0));

        assertArrayEquals(fixture.value(), raw.value(), "value bytes must be returned unchanged (R2.1)");
        assertArrayEquals(fixture.key(), raw.key(), "key surfaced (R2.3)");
        assertArrayEquals(fixture.headers().get("content-type"), raw.headers().get("content-type"),
                "headers surfaced (R2.3)");
        assertEquals(fixture.value().length, raw.serializedSize(), "size surfaced (R2.3)");
        assertTrue(raw.timestamp() >= 0, "timestamp surfaced (R2.3)");
    }

    // ---- R2.1: even undeserializable Confluent-framed bytes come back verbatim ----
    @Test
    void fetchRawBypassesTheFailingDeserializer() throws Exception {
        String topic = "avro-" + System.nanoTime();
        createTopic(topic, 1);
        PoisonFixture fixture = generator.generate(PoisonType.TRUNCATED_AVRO);
        produce(topic, 0, fixture);

        RawMessage raw = newSource().fetchRaw(new TopicPartitionOffset(topic, 0, 0));

        assertArrayEquals(fixture.value(), raw.value());
        assertEquals(0x00, raw.value()[0], "Confluent magic byte preserved (not routed through Avro)");
    }

    // ---- R2.4: a missing offset fails with a clear, actionable message ----
    @Test
    void fetchRawOnMissingOffsetThrowsClearError() throws Exception {
        String topic = "missing-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, generator.generate(PoisonType.NULL_KEY_VALID)); // one message: offset 0 only

        MessageFetchException ex = assertThrows(MessageFetchException.class,
                () -> newSource().fetchRaw(new TopicPartitionOffset(topic, 0, 99)));
        assertTrue(ex.getMessage().toLowerCase().contains("no message at")
                        || ex.getMessage().toLowerCase().contains("holds offsets"),
                "actionable message: " + ex.getMessage());
    }

    // ---- R5.2: the inspect command dumps + classifies end-to-end ----
    @Test
    void inspectCommandDumpsAndClassifies() throws Exception {
        String topic = "cmd-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, generator.generate(PoisonType.JSON_WRONG_TYPE));

        StringWriter out = new StringWriter();
        int code = new CommandLine(new Antidote()).setOut(new PrintWriter(out)).execute(
                "inspect", "--bootstrap", bootstrap(), "--topic", topic, "--partition", "0", "--offset", "0");

        assertEquals(ExitCodes.OK, code);
        String text = out.toString();
        assertTrue(text.contains("Raw payload (hex"), "hex view present: " + text);
        assertTrue(text.contains("[heuristic:"), "classification labeled heuristic: " + text);
        assertTrue(text.contains("Best-effort UTF-8"), "utf-8 view present");
    }

    // ---- R2.4 / R5.5: bad offset -> FETCH_FAILED and a clean message, not a stack trace ----
    @Test
    void inspectCommandFailsCleanlyOnMissingOffset() throws Exception {
        String topic = "cmd-missing-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, generator.generate(PoisonType.NULL_KEY_VALID));

        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int code = new CommandLine(new Antidote())
                .setOut(new PrintWriter(out)).setErr(new PrintWriter(err))
                .execute("inspect", "--bootstrap", bootstrap(), "--topic", topic,
                        "--partition", "0", "--offset", "99");

        assertEquals(ExitCodes.FETCH_FAILED, code);
        String errText = err.toString();
        assertFalse(errText.contains("at com.kafkaantidote"), "no stack trace in output: " + errText);
        assertFalse(errText.contains("Exception"), "no raw exception class in output: " + errText);
        assertTrue(errText.toLowerCase().contains("no message") || errText.toLowerCase().contains("offset"),
                "actionable message: " + errText);
    }

    // ---- R5.6: --json output ----
    @Test
    void inspectCommandEmitsJson() throws Exception {
        String topic = "json-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, generator.generate(PoisonType.JSON_WRONG_TYPE));

        StringWriter out = new StringWriter();
        int code = new CommandLine(new Antidote()).setOut(new PrintWriter(out)).execute(
                "inspect", "--bootstrap", bootstrap(), "--topic", topic,
                "--partition", "0", "--offset", "0", "--json");

        assertEquals(ExitCodes.OK, code);
        String json = out.toString();
        assertTrue(json.contains("\"valueBase64\""), json);
        assertTrue(json.contains("\"classification\""), json);
        assertTrue(json.contains("\"category\""), json);
        assertTrue(json.contains("\"heuristic\":true"), json);
    }

    // ------------------------------ helpers ------------------------------

    private void createTopic(String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private void produce(String topic, int partition, PoisonFixture fixture) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        List<Header> headers = new ArrayList<>();
        fixture.headers().forEach((k, v) -> headers.add(new RecordHeader(k, v)));
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            producer.send(new ProducerRecord<>(topic, partition, fixture.key(), fixture.value(), headers)).get();
        }
    }
}
