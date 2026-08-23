package com.kafkaantidote.harness;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.kafkaantidote.fixtures.PoisonFixture;
import com.kafkaantidote.fixtures.PoisonFixtureGenerator;
import com.kafkaantidote.fixtures.PoisonType;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

/**
 * Phase-0 Definition of Done: a real broker starts, a fixture is produced, and it is read back
 * byte-for-byte (Implementation Plan, Phase 0). This is the harness every later phase builds on.
 *
 * <p>Ground rule G1: a disposable Testcontainers broker only — never production.
 */
@Testcontainers
class KafkaRoundTripIT {

    // GraalVM-native image: the JVM-based apache/kafka image SIGILLs under Docker emulation on
    // Apple Silicon; the native image runs cleanly on arm64 and boots faster.
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    @Test
    void producesAFixtureAndReadsItBackAsRawBytes() {
        PoisonFixture fixture = new PoisonFixtureGenerator().generate(PoisonType.JSON_WRONG_TYPE);
        String topic = "phase0-roundtrip";
        int partition = 0;

        try (KafkaProducer<byte[], byte[]> producer = newProducer()) {
            // Explicit partition: exactly the discipline the Phase-3 re-injection generator will need (R4.2).
            producer.send(new ProducerRecord<>(topic, partition, fixture.key(), fixture.value())).get();
        } catch (Exception e) {
            throw new AssertionError("producing the fixture failed", e);
        }

        try (KafkaConsumer<byte[], byte[]> consumer = newByteConsumer()) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(java.util.List.of(tp));   // assign, not subscribe: no group membership, no rebalance
            consumer.seekToBeginning(java.util.List.of(tp));

            ConsumerRecord<byte[], byte[]> record = pollOne(consumer);
            assertNotNull(record, "expected to read back the produced message");
            assertArrayEquals(fixture.value(), record.value(), "value bytes must round-trip unchanged");
            assertArrayEquals(fixture.key(), record.key(), "key bytes must round-trip unchanged");
            assertEquals(0L, record.offset(), "first message lands at offset 0");
        }
    }

    private static ConsumerRecord<byte[], byte[]> pollOne(KafkaConsumer<byte[], byte[]> consumer) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        return null;
    }

    private static KafkaProducer<byte[], byte[]> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    private static KafkaConsumer<byte[], byte[]> newByteConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // read-only discipline (R1.4/R6)
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "phase0-inspector-" + System.nanoTime());
        return new KafkaConsumer<>(p);
    }
}
