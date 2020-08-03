/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.stream.Collectors;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

  private static final boolean IS_VANILLA = false;
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
    if (IS_VANILLA) {
      amqServer = TestSupport.createVanillaEmbeddedAmq(b -> b
          .dataDirectory(amqDataDir.toString())
          .jmsBridgeProps(kafkaContainer.defaultProps()));
    } else {
      amqServer = TestSupport.createEmbeddedAmq(b -> b
          .jmsBridgeProps(kafkaContainer.defaultProps())
          .dataDirectory(amqDataDir.toString()));
    }

    amqServer.start();
  }

  @AfterAll
  public static void cleanupAll() throws Exception {
    amqServer.stop();
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

    //allow the consumer to get situated.
    Thread.sleep(1000);

    int count = 1;
    try {
      for (int i = 0; i < count; i++) {
        producer.send(session.createTextMessage("Hello JMS Bridge " + i));
        Message received = consumer.receive(100);
        assertNotNull(received);
        assertEquals("Hello JMS Bridge " + i, received.getBody(String.class));
        Thread.sleep(100);
      }
    } finally {
      System.out.println("Closing JMS session and connection");
      consumer.close();
      session.close();
    }

    if (!IS_VANILLA) {
      String bindingsJournal = "_jms.bridge_junit_bindings";
      logJournalFiles(bindingsJournal);

      String messagesJournal = "_jms.bridge_junit_messages";
      logJournalFiles(messagesJournal);
    }
  }

  public void logJournalFiles(String journalTopic) {
    String journalStr = kafkaContainer
        .consumeAll(journalTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        .stream()
        .map(r -> {

          String key = "null";
          if (r.key() != null) {
            try {
              JournalRecordKey rkey = JournalRecordKey.parseFrom(r.key());
              key = String.format("tx-%d_id-%d", rkey.getTxId(), rkey.getId());
            } catch (Exception e) {
              key = "ERROR";
            }
          }

          String value = "null";
          if (r.value() != null) {
            try {
              JournalRecord rval = JournalRecord.parseFrom(r.value());
              value = String.format("RecordType: %s, UserRecordType: %d",
                  rval.getRecordType().name(), rval.getUserRecordType());
            } catch (Exception e) {
              value = "ERROR";
            }
          }

          return String.format("Offset: %d, Key: %s, Value: %s", r.offset(), key, value);
        }).collect(Collectors.joining(System.lineSeparator() + System.lineSeparator()));

    System.out.println(
        "#### JOURNAL FOR TOPIC " + journalTopic + " ####" + System.lineSeparator() + journalStr);
  }
}
