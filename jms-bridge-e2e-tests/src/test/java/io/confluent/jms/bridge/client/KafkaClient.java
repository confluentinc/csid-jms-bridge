package io.confluent.jms.bridge.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.jms.bridge.util.constants.Constants;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class KafkaClient {
    public static void kafkaProducer(HashMap<String, String> server, String destination) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(destination, "0", "Hello Development Team");

        producer.send(record, (metadata, exception) -> {
            System.out.println("Message producing");
            if (exception == null) {
                System.out.println("Message sent successfully: " + record.value() + " to topic " + record.topic() + " at offset " + metadata.offset());
            } else {
                System.err.println("Error sending message: " + exception.getMessage());
            }
        });

        // Ensure the producer is closed properly to free resources
        producer.close();
    }
    public static void kafkaConsumer(HashMap<String, String> server, String destination) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));  // Updated to use ConsumerConfig
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "quick-start1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(destination));
        System.out.println("Kafka Receiver Start : ");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            records.forEach(record -> {
                System.out.println("Consuming");
                System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                        record.key(), record.value(), record.partition(), record.offset());
            });
        }
    }
}
