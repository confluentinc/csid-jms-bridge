package org.example.ckafka.staticprops;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
//import java.org.example.util.Scanner;

public class CKafkaProducerCloud {

    public Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = CKafkaProducerCloud.class.getClassLoader().getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }


    public static void main(String[] args) throws IOException {
        // Set up producer properties
        final Properties properties = new CKafkaProducerCloud().loadConfig("client.properties");
        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>("TestTopic", "key", "Sample test msg"));
        producer.close();
    }
}
