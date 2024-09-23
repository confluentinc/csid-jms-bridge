package org.example.ckafka.staticprops;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
//import java.org.example.util.Scanner;

public class CKafkaProducerJAASCloud {
    public static void main(String[] args) {
        String username = "GZTZLR3FAOSOREU2";
        String password = "SANwUdxfHadSPEy+70i9qmwIYtIvE5jy1R3C888ZeLFst4psDuvFRvgMVVAl+hUg";
        // Set up producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092");
        //properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord("quick-start" , "0", "Confluent cloud Testing message ");
        //producer.send(record);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully: " +record.value() +"   " + metadata.offset());
            } else {
                System.err.println("Error sending message: " + exception.getMessage());
            }
        });

        // Close the producer
        producer.close();
    }
}