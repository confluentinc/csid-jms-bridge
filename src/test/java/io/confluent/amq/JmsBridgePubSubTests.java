/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
@Disabled
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
  @Timeout(30)
  public void kafkaMessageKeysAreCorrect() throws Exception {
    if (IS_VANILLA) {
      return;
    }

    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageProducer producer = session.createProducer(topic);

    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");
    //allow the consumer to get situated.
    Thread.sleep(100);

    //create enough messages to ensure IDs are increasing
    int count = 100;
    try {
      for (int i = 0; i < count; i++) {
        producer.send(session.createTextMessage("Hello JMS Bridge " + i));
      }

      for (int i = 0; i < count; i++) {
        Message received = consumer.receive(100);
        assertNotNull(received);
        assertEquals("Hello JMS Bridge " + i, received.getBody(String.class));
      }

    } finally {
      System.out.println("Closing JMS session and connection");
      consumer.close();
      session.close();
    }

    KafkaIO kafkaIo = new KafkaIO(kafkaContainer.defaultProps());
    kafkaIo.start();

    DescribeJournal.printSurvivingRecords(
        new KafkaJournal(
            kafkaIo,
            "junit",
            "messages",
            null,
            (a, b, c) -> {

            }), System.out, true);

    String messagesJournal = "_jms.bridge_junit_messages";
    List<JournalRecordKey> keys = streamJournalFiles(messagesJournal)
        //tombstones
        .filter(p -> p.getValue() != null)
        //exclude TX
        .filter(p -> p.getValue().getRecordType() == JournalRecordType.ADD_RECORD)
        .map(Pair::getKey)
        .collect(Collectors.toList());
    Set<Long> foundIds = new HashSet<>();
    for (JournalRecordKey key : keys) {
      assertFalse(String.format("Duplicate ID found: '%d'", key.getId()),
          foundIds.contains(key.getId()));
      foundIds.add(key.getId());
    }

    DescribeJournal.printSurvivingRecords(
        new KafkaJournal(
            new KafkaIO(kafkaContainer.defaultProps()),
            "junit",
            "messages",
            null,
            (a, b, c) -> {

            }), System.out, true);
  }

  @Test
  @Timeout(30)
  public void jmsBasicPubSub() throws Exception {
    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    //allow the consumer to get situated.
    Thread.sleep(1000);

    int count = 100;
    try {
      for (int i = 0; i < count; i++) {
        producer.send(session.createTextMessage("Hello JMS Bridge " + i));
      }

      for (int i = 0; i < count; i++) {
        Message received = consumer.receive(100);
        assertNotNull(received);
        assertEquals("Hello JMS Bridge " + i, received.getBody(String.class));
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

  public Stream<Pair<JournalRecordKey, JournalRecord>> streamJournalFiles(String journalTopic) {
    return kafkaContainer
        .consumeAll(journalTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        .stream()
        .map(r -> {

          JournalRecordKey rkey = null;
          if (r.key() != null) {
            try {
              rkey = JournalRecordKey.parseFrom(r.key());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          JournalRecord rval = null;
          if (r.value() != null) {
            try {
              rval = JournalRecord.parseFrom(r.value());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return Pair.of(rkey, rval);
        });
  }

  public void logJournalFiles(String journalTopic) {
    String journalStr = streamJournalFiles(journalTopic)
        .map(pair -> {

          String key = "null";
          if (pair.getKey() != null) {
            try {
              key = String.format("tx-%d_id-%d", pair.getKey().getTxId(), pair.getKey().getId());
            } catch (Exception e) {
              key = "ERROR";
            }
          }

          String value = "null";
          if (pair.getValue() != null) {
            try {
              value = String.format("RecordType: %s, UserRecordType: %d",
                  pair.getValue().getRecordType().name(), pair.getValue().getUserRecordType());
            } catch (Exception e) {
              value = "ERROR";
            }
          }

          return String.format("Key: %s, Value: %s", key, value);
        }).collect(Collectors.joining(System.lineSeparator() + System.lineSeparator()));

    System.out.println(
        "#### JOURNAL FOR TOPIC " + journalTopic + " ####" + System.lineSeparator() + journalStr);
  }
}
