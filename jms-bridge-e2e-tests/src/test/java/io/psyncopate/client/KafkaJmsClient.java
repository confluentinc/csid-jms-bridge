package io.psyncopate.client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.util.constants.Constants;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class KafkaJmsClient {
    private static final Logger logger = LogManager.getLogger(KafkaJmsClient.class);

    public static int kafkaProducer(HashMap<String, String> server, String destination, int messageCountToBeSent) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Send multiple message and display
        Producer<String, String> producer = new KafkaProducer<>(properties);

        int sentMessageCount = 0;
        for (;(messageCountToBeSent == -1 || sentMessageCount < messageCountToBeSent) ; sentMessageCount++) {
            String message = "Hello Development Team " + sentMessageCount;

            // Create a ProducerRecord with the destination topic, key (optional), and message
            ProducerRecord<String, String> record = new ProducerRecord<>(destination, Integer.toString(sentMessageCount), message);

            // Send the message asynchronously
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    int i=0;
                    logger.debug("Message sent successfully: "+ i + record.value() + " to topic "
                            + record.topic() + " at offset " + metadata.offset());
                } else {
                    logger.debug("Error sending message: " + exception.getMessage());
                }
            });
        }

        // Ensure the producer is closed properly to free resources
        producer.close();
        return sentMessageCount;
    }


    public static int kafkaConsumer(HashMap<String, String> server, String topic, int messageCountToBeConsumed) {
        // Set idle time to 5 seconds
        final long idleTimeDurationInMillis = 5000; // 5 seconds in milliseconds

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka Consumer instance
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic));

        int consumedMessageCount = 0;
        long lastMessageTime = System.currentTimeMillis(); // Track the last message time

        try {
            // Poll messages in a loop
            while (messageCountToBeConsumed == -1 || consumedMessageCount < messageCountToBeConsumed) {
                // Poll for new messages with a timeout of 100ms
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    // If no messages are received, check idle time
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastMessageTime >= idleTimeDurationInMillis) {
                        // If idle for 5 seconds, break the loop
                        System.out.println("No messages received for 5 seconds, closing the consumer.");
                        break;
                    }
                } else {
                    // Reset the idle timer since a message was received
                    lastMessageTime = System.currentTimeMillis();

                    // Process each record
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Consumed message: key = %s, value = %s, topic = %s, partition = %s, offset = %d%n",
                                record.key(), record.value(), record.topic(), record.partition(), record.offset());
                        consumedMessageCount++;

                        // If the desired message count is reached, break the loop
                        if (messageCountToBeConsumed != -1 && consumedMessageCount >= messageCountToBeConsumed) {
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error consuming messages: " + e.getMessage());
        } finally {
            // Ensure the consumer is closed properly to free resources
            consumer.close();
        }

        return consumedMessageCount;
    }


//    public static void kafkaConsumer(HashMap<String, String> server, String destination) {
//        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));  // Updated to use ConsumerConfig
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "quick-start1");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//
//        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
//        consumer.subscribe(Collections.singletonList(destination));
//        System.out.println("Kafka Receiver Start : ");
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
//            records.forEach(record -> {
//                System.out.println("Consuming");
//                System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
//                        record.key(), record.value(), record.partition(), record.offset());
//            });
//        }
//    }
}
