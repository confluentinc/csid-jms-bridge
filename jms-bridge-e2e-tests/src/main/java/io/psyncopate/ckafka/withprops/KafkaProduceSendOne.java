package io.psyncopate.ckafka.withprops;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
public class KafkaProduceSendOne {
    public static void main(String[] args) {
        String[] producerKeys = new String[]{
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
        };
        Properties allProps = getProperties();
        assert allProps != null;
        Properties properties = getProperties(allProps, producerKeys);
        properties.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .forEach(System.out::println);
        Producer<String, String> producer = new KafkaProducer<>(properties);

        String topic = allProps.getProperty("topic");
        String key = "my_key";
        String value = "Hello, Kafka!";


        ProducerRecord<String, String> record = new ProducerRecord(topic , key, value);
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
