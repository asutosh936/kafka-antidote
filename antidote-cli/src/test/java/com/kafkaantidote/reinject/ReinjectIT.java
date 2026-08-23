package com.kafkaantidote.reinject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkaantidote.cli.Antidote;
import com.kafkaantidote.cli.ExitCodes;
import com.kafkaantidote.consumer.ConsumerMessageSource;
import com.kafkaantidote.core.RawMessage;
import com.kafkaantidote.core.ReinjectionPlan;
import com.kafkaantidote.core.TopicPartitionOffset;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import picocli.CommandLine;

/**
 * The R4 safety contract against a real broker: the generated script is COMPILED and RUN — dry-run
 * produces nothing, and {@code --produce --yes} lands the corrected message on the EXACT original
 * partition (verified by consuming it back). Ground rule G1: disposable broker only.
 */
@Testcontainers
class ReinjectIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    @TempDir
    Path tempDir;

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    private ConsumerMessageSource source() {
        return new ConsumerMessageSource(bootstrap(), 1, Duration.ZERO);
    }

    @Test
    void dryRunProducesNothingAndRealRunLandsOnExactPartition() throws Exception {
        String topic = "reinject-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, "order-1".getBytes(StandardCharsets.UTF_8),
                "broken".getBytes(StandardCharsets.UTF_8));

        byte[] corrected = "{\"id\":\"order-1\",\"amount\":42}".getBytes(StandardCharsets.UTF_8);
        RawMessage original = source().fetchRaw(new TopicPartitionOffset(topic, 0, 0));
        ReinjectionPlan plan = source().planReinjection(original, corrected);

        Class<?> generated = compile(plan.scriptContent());

        // --- Dry run: must produce nothing (R4.4). Log end stays at 1. ---
        String dryOutput = runMain(generated, new String[] {});
        assertTrue(dryOutput.contains("DRY RUN"), "dry run announced: " + dryOutput);
        assertEquals(1L, endOffset(topic, 0), "dry run must not produce anything");

        // --- Real run: produces the corrected message to the EXACT partition (R4.2). ---
        String realOutput = runMain(generated, new String[] {"--produce", "--yes"});
        assertTrue(realOutput.contains("Produced corrected message to " + topic + "-0@1"),
                "produced to exact partition: " + realOutput);
        assertEquals(2L, endOffset(topic, 0), "exactly one new message appended");

        byte[] readBack = consumeValueAt(topic, 0, 1);
        assertArrayEquals(corrected, readBack, "the corrected bytes landed on partition 0, offset 1");
    }

    @Test
    void genReinjectCommandWritesAReviewableScript() throws Exception {
        String topic = "cmd-reinject-" + System.nanoTime();
        createTopic(topic, 1);
        produce(topic, 0, "k".getBytes(StandardCharsets.UTF_8), "broken".getBytes(StandardCharsets.UTF_8));

        Path corrected = tempDir.resolve("fixed.bin");
        Files.write(corrected, "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));
        Path outFile = tempDir.resolve("Reinject.java");

        StringWriter out = new StringWriter();
        int code = new CommandLine(new Antidote()).setOut(new PrintWriter(out)).execute(
                "gen-reinject", "--bootstrap", bootstrap(), "--topic", topic,
                "--partition", "0", "--offset", "0",
                "--corrected-payload", corrected.toString(), "--out", outFile.toString());

        assertEquals(ExitCodes.OK, code);
        String script = Files.readString(outFile);
        assertTrue(script.contains("PARTITION = 0"), "targets the exact partition");
        assertTrue(script.contains("WARNING"), "carries the warning header");
        assertTrue(out.toString().toLowerCase().contains("review"), "reminds the operator to review");
    }

    // ------------------------------ compile & run the generated source ------------------------------

    private Class<?> compile(String source) throws Exception {
        Path srcDir = Files.createDirectories(tempDir.resolve("src"));
        Path classesDir = Files.createDirectories(tempDir.resolve("classes"));
        Path srcFile = srcDir.resolve("Reinject.java");
        Files.writeString(srcFile, source);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream diag = new ByteArrayOutputStream();
        int rc = compiler.run(null, null, diag,
                "-classpath", System.getProperty("java.class.path"),
                "-d", classesDir.toString(),
                srcFile.toString());
        assertEquals(0, rc, "generated script must compile cleanly:\n" + diag);

        URLClassLoader loader = new URLClassLoader(
                new URL[] {classesDir.toUri().toURL()}, getClass().getClassLoader());
        return Class.forName("Reinject", true, loader);
    }

    private String runMain(Class<?> generated, String[] args) throws Exception {
        Method main = generated.getMethod("main", String[].class);
        PrintStream originalOut = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            main.invoke(null, (Object) args);
        } finally {
            System.setOut(originalOut);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    // ------------------------------ broker helpers ------------------------------

    private void createTopic(String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private void produce(String topic, int partition, byte[] key, byte[] value) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps())) {
            producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        }
    }

    private long endOffset(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(byteConsumerProps())) {
            c.assign(List.of(tp));
            return c.endOffsets(List.of(tp)).getOrDefault(tp, -1L);
        }
    }

    private byte[] consumeValueAt(String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(byteConsumerProps())) {
            c.assign(List.of(tp));
            c.seek(tp, offset);
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = c.poll(Duration.ofMillis(500));
                for (var rec : records.records(tp)) {
                    if (rec.offset() == offset) {
                        return rec.value();
                    }
                }
            }
        }
        throw new AssertionError("no record at " + topic + "-" + partition + "@" + offset);
    }

    private Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    private Properties byteConsumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "reinject-it-" + System.nanoTime());
        return p;
    }
}
