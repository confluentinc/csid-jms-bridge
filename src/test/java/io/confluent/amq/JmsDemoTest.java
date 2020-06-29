//package io.confluent.amq;
//
//import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.junit.Test;
//
//import javax.jms.*;
//import java.util.List;
//
//import static org.junit.Assert.*;
//import static org.junit.Assert.assertEquals;
//
//public class JmsDemoTest {
//    @Test
//    public void jmsPublish() throws Exception {
//        String topicName = "jms-to-kafka";
//
//        ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory("tcp://localhost:61616", "unit-test");
//        Connection amqConnection = cf.createConnection();
//        amqConnection.setClientID("test-client-id");
//
//        try {
//            amqConnection.start();
//
//            Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            Topic topic = session.createTopic(topicName);
//            MessageProducer producer = session.createProducer(topic);
//
//            //without a consumer the message isn't routable so it will never be stored
//            //It also must be targetting a durable queue
//            MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");
//
//            TextMessage message = session.createTextMessage("Hello Kafka");
//            message.setJMSCorrelationID("yo-correlate-man");
//            //Exceptions in bridges do bubble up to the client.
//            producer.send(message);
//
//
//            Message received = consumer.receive(100);
//            assertEquals("Hello Kafka", received.getBody(String.class));
//        } finally {
//            producer.close();
//            consumer.close();
//            session.close();
//        }
//
//        List<ConsumerRecord<String, String>> records = consumeAllRecords(topicName, stringSerde.deserializer(), stringSerde.deserializer());
//        assertEquals(1, records.size());
//        assertEquals("Hello Kafka", records.get(0).value());
//        assertEquals("yo-correlate-man", records.get(0).key());
//        assertTrue(records.get(0).headers().toArray().length > 0);
//
//    }
//
//    @Test
//    public void kafkaPublishJmsConsumeTopic() throws Exception {
//        String topicName = "jms-to-kafka";
//        createKafkaTopic(topicName, 1);
//
//        Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//        Topic topic = session.createTopic(topicName);
//        MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");
//
//        //allow the consumer to get situated.
//        Thread.sleep(1000);
//        RecordMetadata meta = kafkaProducer.send(new ProducerRecord<>(topicName, "key".getBytes(), "Hello AMQ".getBytes())).get();
//
//        try {
//            Message received = consumer.receive(5000);
//            assertNotNull(received);
//            assertEquals("Hello AMQ", new String(received.getBody(byte[].class)));
//        } finally {
//            consumer.close();
//            session.close();
//        }
//
//        List<ConsumerRecord<String, String>> records = consumeAllRecords(topicName, stringSerde.deserializer(), stringSerde.deserializer());
//        assertEquals(1, records.size());
//
//
//    }
//}
