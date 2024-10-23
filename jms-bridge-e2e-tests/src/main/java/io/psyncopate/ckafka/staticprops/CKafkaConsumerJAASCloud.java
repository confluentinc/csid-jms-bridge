package io.psyncopate.ckafka.staticprops;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CKafkaConsumerJAASCloud {
    public static void main(String[] args) {
        String username = "GZTZLR3FAOSOREU2";
        String password = "SANwUdxfHadSPEy+70i9qmwIYtIvE5jy1R3C888ZeLFst4psDuvFRvgMVVAl+hUg";
        // Set up consumer properties
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "quick-start-kafka-integration-test-4");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create a Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic(s) you want to consume from
        consumer.subscribe(Collections.singletonList("quick-start"));
        System.out.println("Kafka Receiver Start : ");
        while (true) {
            ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofMillis(100L));
            records1.forEach(record -> {
                System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                        record.key(), record.value(), record.partition(), record.offset());
            });
        }
    }
}