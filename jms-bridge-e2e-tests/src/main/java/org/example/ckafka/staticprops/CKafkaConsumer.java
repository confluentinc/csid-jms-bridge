package org.example.ckafka.staticprops;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CKafkaConsumer {
    public static void main(String[] args) {
        // Set up consumer properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "jms-bridge-test-vm2.psyncopate.io:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Group-pst5");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create a Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic(s) you want to consume from
        consumer.subscribe(Collections.singletonList("test-gp-top2"));
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