/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static io.confluent.amq.SerdePool.deserString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@Tag("IntegrationTest")
public class ConfluentAmqServerTests {

  private static final String JMS_TOPIC = "jms-to-kafka";
  static final Serde<String> stringSerde = Serdes.String();

  @RegisterExtension
  public static KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.4.0")
        .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  public static ConfluentEmbeddedAmqImpl amqServer;

  @TempDir
  public static Path amqDataDir;

  Connection amqConnection;

  @BeforeAll
  public static void setupAll() throws Exception {
    kafkaContainer.createTopic(JMS_TOPIC, 1);

    Properties kafkaProps = kafkaContainer.defaultProps();
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "amq-server");
    amqServer = new ConfluentEmbeddedAmqImpl.Builder(kafkaProps).build();

    Configuration amqConf = amqServer.getAmq().getConfiguration();
    amqConf.setBindingsDirectory(
        Files.createDirectory(amqDataDir.resolve("bindings")).toAbsolutePath().toString());
    amqConf.setJournalDirectory(
        Files.createDirectory(amqDataDir.resolve("journal")).toAbsolutePath().toString());
    amqConf.setPagingDirectory(
        Files.createDirectory(amqDataDir.resolve("Paging")).toAbsolutePath().toString());
    amqConf.setLargeMessagesDirectory(
        Files.createDirectory(amqDataDir.resolve("large-messages")).toAbsolutePath().toString());
    amqConf.setNodeManagerLockDirectory(
        Files.createDirectory(amqDataDir.resolve("node-manager-lock")).toAbsolutePath().toString());

    amqServer.start();
  }

  @AfterAll
  public static void cleanupAll() throws Exception {
    amqServer.stop();
    kafkaContainer.deleteTopics(JMS_TOPIC);
  }

  @BeforeEach
  public void setup() throws Exception {
    ConnectionFactory cf = ActiveMQJMSClient
        .createConnectionFactory("tcp://localhost:61616", "unit-test");
    amqConnection = cf.createConnection();
    amqConnection.setClientID("test-client-id");
    amqConnection.start();
  }

  @AfterEach
  public void cleanup() throws Exception {
    amqConnection.close();
  }

  @Test
  public void jmsPublishKafkaConsumeTopic() throws Exception {
    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    TextMessage message = session.createTextMessage("Hello Kafka");
    message.setJMSCorrelationID("yo-correlate-man");
    //Exceptions in bridges do bubble up to the client.
    producer.send(message);

    Thread.sleep(1000);

    try {
      Message received = consumer.receive(100);
      assertEquals("Hello Kafka", received.getBody(String.class));
    } finally {
      producer.close();
      consumer.close();
      session.close();
    }

    List<ConsumerRecord<String, String>> records = kafkaContainer.consumeAll(JMS_TOPIC,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());
    assertEquals("Hello Kafka", records.get(0).value());
    assertEquals("yo-correlate-man", records.get(0).key());
    assertTrue(records.get(0).headers().toArray().length > 0);

  }

  @Test
  public void kafkaPublishJmsConsumeTopic() throws Exception {
    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    //allow the consumer to get situated.
    Thread.sleep(1000);
    kafkaContainer.publish(JMS_TOPIC, "key", "Hello AMQ");

    try {
      Message received = consumer.receive(5000);
      assertNotNull(received);
      assertEquals("Hello AMQ",
          deserString(received.getBody(byte[].class)));
    } finally {
      consumer.close();
      session.close();
    }

    List<ConsumerRecord<String, String>> records = kafkaContainer.consumeAll(JMS_TOPIC,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());
  }

}
