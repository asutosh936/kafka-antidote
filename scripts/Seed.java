// Seeds a poison-pill scenario into localhost:9092 for MANUAL testing of Kafka Antidote.
//
// Produces 5 messages to topic 'orders' partition 0, with a real poison pill (JSON wrong-types)
// at offset 2, then parks consumer group 'orders-consumer' at committed offset 2 (lag 3) so the
// group looks genuinely stuck.
//
// Run it on your host JDK (arm64-native, no SIGILL), reusing the fat jar for kafka-clients:
//   java -cp antidote-cli/target/antidote.jar scripts/Seed.java
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Seed {
    static final String BOOTSTRAP = "localhost:9092";
    static final String TOPIC = "orders";
    static final String GROUP = "orders-consumer";

    public static void main(String[] args) throws Exception {
        Properties ap = new Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        try (Admin admin = Admin.create(ap)) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            } catch (Exception alreadyExists) {
                // fine if the topic is already there
            }
        }

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> pr = new KafkaProducer<>(pp)) {
            for (int i = 0; i < 5; i++) {
                byte[] key = ("order-" + i).getBytes(StandardCharsets.UTF_8);
                byte[] value = (i == 2)
                        ? "{\"id\":\"order-2\",\"amount\":true,\"quantity\":\"seven\"}".getBytes(StandardCharsets.UTF_8)
                        : ("{\"id\":\"order-" + i + "\",\"amount\":" + (i * 10) + "}").getBytes(StandardCharsets.UTF_8);
                pr.send(new ProducerRecord<>(TOPIC, 0, key, value)).get();
            }
        }

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            c.assign(List.of(tp));
            c.commitSync(Map.of(tp, new OffsetAndMetadata(2)));
        }

        System.out.println("Seeded: 5 messages on " + TOPIC + "-0, poison pill at offset 2, "
                + "group '" + GROUP + "' parked at committed offset 2 (lag 3).");
    }
}
