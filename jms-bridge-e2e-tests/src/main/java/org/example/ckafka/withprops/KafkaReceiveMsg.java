package org.example.ckafka.withprops;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaReceiveMsg {
    public static void main(String[] args) {
        String[] producerKeys = new String[]{
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConsumerConfig.GROUP_ID_CONFIG,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
        };
        Properties allProps = getProperties();
        assert allProps != null;
        Properties properties = getProperties(allProps, producerKeys);
        properties.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .forEach(System.out::println);

        // Create a Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic(s) you want to consume from

        consumer.subscribe(Collections.singletonList(allProps.getProperty("topic")));
        System.out.println("Kafka Receiver Start : ");
        int messageCount = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0L));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Received message %d : key = %s, value = %s, offset = %d, partition=%d %n",
                        messageCount, record.key(), record.value(), record.offset(), record.partition());
                messageCount++;
            }
        }
    }

    private static Properties getProperties() {
        // Set up consumer properties
        ClassLoader classLoader = KafkaReceiveMsg.class.getClassLoader();
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(classLoader.getResource("kafka-config.properties").getPath())) {
            props.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return props;
    }
    private static Properties getProperties(Properties allProps, String[] producerKeys) {
        Properties properties = new Properties();
        for (String key : producerKeys) {
            String value = allProps.getProperty(key);
            properties.put(key, value);
            //System.out.println("Key: " + key + ", Value: " + value);
        }
        return properties;
    }
}