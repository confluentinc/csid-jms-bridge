/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static io.confluent.amq.test.TestSupport.println;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgePubSubTests {

  private static final boolean IS_VANILLA = false;
  private static final String JMS_TOPIC = "jms-to-kafka";

  @RegisterExtension
  @Order(100)
  public static KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  @RegisterExtension
  @Order(200)
  public static ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
      .useVanilla(IS_VANILLA)
      .jmsBridgeProps(kafkaContainer.defaultProps()));


  @Test
  @Timeout(30)
  public void idGeneratorIsUnique() throws Exception {
    if (IS_VANILLA) {
      return;
    }

    Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
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
      println("Closing JMS session and connection");
      consumer.close();
      session.close();
    }

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
  }

  @Test
  @Timeout(30)
  public void jmsClientTxRollback() throws Exception {
    String topicName = "jms-client-tx-rollback";
    String subscriberName = "jms-client-tx-rollback-subscriber";

    try (
        Session txSession = amqServer.getConnection().createSession(true, Session.AUTO_ACKNOWLEDGE);
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = txSession.createTopic(topicName);

      try (
          MessageProducer producer = txSession.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        producer.send(txSession.createTextMessage("Message 1"));
        producer.send(txSession.createTextMessage("Message 2"));
        txSession.rollback();

        Message rcvmsg = consumer.receive(100);
        assertNull(rcvmsg);
      }
    }
  }

  @Test
  @Timeout(30)
  public void jmsClientPubSubMultipleBindings() throws Exception {
    String topicName = "jms-client-tx-commit";
    String subscriberName = "jms-client-tx-commit-subscriber";

    try (
        Session session1 = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session1.createTopic(topicName);

      try (
          MessageProducer producer = session1.createProducer(topic);
          MessageConsumer consumer1 = session1.createDurableConsumer(topic, subscriberName + "-1");
          MessageConsumer consumer2 = session2.createDurableConsumer(topic, subscriberName + "-2")
      ) {

        producer.send(session1.createTextMessage("Message 1"));
        Message rcvmsg1 = consumer1.receive(100);
        Message rcvmsg2 = consumer2.receive(100);

        assertEquals("Message 1", rcvmsg1.getBody(String.class));
        assertEquals("Message 1", rcvmsg2.getBody(String.class));
      }
    }
  }

  @Test
  @Timeout(30)
  public void jmsClientTxCommit() throws Exception {
    String topicName = "jms-client-tx-commit";
    String subscriberName = "jms-client-tx-commit-subscriber";

    try (
        Session txSession = amqServer.getConnection().createSession(true, Session.AUTO_ACKNOWLEDGE);
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = txSession.createTopic(topicName);

      try (
          MessageProducer producer = txSession.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        producer.send(txSession.createTextMessage("Message 1"));
        Message rcvmsg = consumer.receive(100);
        assertNull(rcvmsg);

        producer.send(txSession.createTextMessage("Message 2"));
        txSession.commit();

        rcvmsg = consumer.receive(100);
        assertEquals("Message 1", rcvmsg.getBody(String.class));

        rcvmsg = consumer.receive(100);
        assertEquals("Message 2", rcvmsg.getBody(String.class));

      }
    }
  }

  @Test
  @Timeout(30)
  public void jmsBasicPubSub() throws Exception {
    Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
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
      println("Closing JMS session and connection");
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

    println(
        "#### JOURNAL FOR TOPIC " + journalTopic + " ####" + System.lineSeparator() + journalStr);
  }
}
