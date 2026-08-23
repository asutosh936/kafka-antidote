package com.kafkaantidote.demo;

import com.kafkaantidote.cli.Antidote;
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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import picocli.CommandLine;

/**
 * A watch-it-live demo (NOT part of the normal suite). It boots a disposable broker, creates a
 * genuinely stuck consumer group, and runs the real {@code diagnose} command so its output prints
 * to your terminal.
 *
 * <p>Gated behind {@code -Dantidote.demo=true} at the class level, so a normal {@code mvn verify}
 * skips it entirely (no container is started). Run it with:
 *
 * <pre>
 * mvn -q -pl antidote-cli -am test-compile failsafe:integration-test \
 *     -Dit.test=DiagnoseDemoIT -Dantidote.demo=true
 * </pre>
 */
@Testcontainers
@EnabledIfSystemProperty(named = "antidote.demo", matches = "true")
class DiagnoseDemoIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.1");

    @Test
    void showDiagnoseAgainstAStuckGroup() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        String topic = "orders";
        String group = "orders-consumer";

        // Set the scene: 5 messages produced, group consumed 2 then stalled -> lag 3 on partition 0.
        createTopic(bootstrap, topic, 1);
        produce(bootstrap, topic, 0, 5);
        setCommitted(bootstrap, group, topic, 0, 2);

        System.out.println();
        System.out.println("=========================================================");
        System.out.println(" LIVE DEMO — a consumer group stuck at offset 2 (lag 3)");
        System.out.println(" $ antidote diagnose --bootstrap " + bootstrap + " --group " + group);
        System.out.println("=========================================================");

        int code = new CommandLine(new Antidote()).execute(
                "diagnose", "--bootstrap", bootstrap, "--group", group,
                "--poll-interval", "200ms", "--samples", "2");

        System.out.println("---------------------------------------------------------");
        System.out.println(" exit code = " + code + "   (0=clean, 1=poison detected)");
        System.out.println("=========================================================");
        System.out.println();
    }

    private void createTopic(String bootstrap, String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private void produce(String bootstrap, String topic, int partition, int count) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, partition, null,
                        ("order-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))).get();
            }
        }
    }

    private void setCommitted(String bootstrap, String group, String topic, int partition, long offset) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
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
