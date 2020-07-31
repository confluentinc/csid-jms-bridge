/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static io.confluent.amq.SerdePool.deserString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.List;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
public class JmsBridgePubSubTests {
  private static final String JMS_TOPIC = "jms-to-kafka";

  @RegisterExtension
  public static KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  public static ConfluentEmbeddedAmq amqServer;

  @TempDir
  public static Path amqDataDir;

  Connection amqConnection;

  @BeforeAll
  public static void setupAll() throws Exception {
    kafkaContainer.createTopic(JMS_TOPIC, 1);

    amqServer = TestSupport.createEmbeddedAmq(b -> b
        .jmsBridgeProps(kafkaContainer.defaultProps())
        .dataDirectory(amqDataDir.toString()));

    amqServer.start();
  }

  @AfterAll
  public static void cleanupAll() throws Exception {
    amqServer.stop();
    kafkaContainer.deleteTopics(JMS_TOPIC);
  }

  @BeforeEach
  public void setup() throws Exception {
    amqConnection = TestSupport.startJmsConnection(b -> {

    });
  }

  @AfterEach
  public void cleanup() throws Exception {
    amqConnection.close();
  }


  @Test
  public void jmsBasicPubSub() throws Exception {
    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    TextMessage message = session.createTextMessage("Hello JMS Bridge");
    //Exceptions in bridges do bubble up to the client.
    producer.send(message);

    //allow the consumer to get situated.
    Thread.sleep(1000);

    try {
      Message received = consumer.receive(5000);
      assertNotNull(received);
      assertEquals("Hello JMS Bridge", received.getBody(String.class));
    } finally {
      consumer.close();
      session.close();
    }
  }
}
