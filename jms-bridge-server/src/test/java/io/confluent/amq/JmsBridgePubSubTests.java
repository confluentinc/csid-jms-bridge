/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static io.confluent.amq.persistence.domain.proto.JournalRecordType.ADD_RECORD;
import static io.confluent.amq.test.TestSupport.getCompactedJournal;
import static io.confluent.amq.test.TestSupport.println;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgePubSubTests {

  private static final boolean IS_VANILLA = false;
  private static final String JMS_TOPIC = "jms-to-kafka";

  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  @RegisterExtension
  @Order(300)
  public static final ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
      .useVanilla(IS_VANILLA)
      .jmsBridgeConfigBuilder()
        .putAllKafka(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps()))
      .putStreams(StreamsConfig.STATE_DIR_CONFIG, tempdir.toAbsolutePath().toString()));

  @Test
  @Timeout(30)
  public void idGeneratorIsUniqueAcrossReloads() throws Exception {
    if (IS_VANILLA) {
      return;
    }

    assertIdsUnique();
    amqServer.restartServer();
    assertIdsUnique();
  }

  public void assertIdsUnique() throws Exception {
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
    TestSupport.logJournalFiles(kafkaContainer, messagesJournal, true);

    Map<Long, Long> keyCounts = streamJournalFiles(messagesJournal)
        //tombstones
        .filter(p -> p.getValue() != null)
        //exclude TX
        .filter(p -> p.getValue().hasAppendedRecord())
        .filter(p -> p.getValue().getAppendedRecord().getRecordType() == ADD_RECORD)
        .map(Pair::getKey)
        .collect(Collectors.groupingBy(JournalEntryKey::getMessageId, Collectors.counting()));

    keyCounts.entrySet().stream()
        .filter(en -> en.getValue() > 1)
        .forEach(en ->
            println("Duplicate messageId found, ID: {}, dupeCount: {}",
                en.getKey(), en.getValue()));

    assertEquals(
        0L,
        keyCounts.values().stream().filter(c -> c > 1).count(),
        "Duplicate IDs found.");
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

    String messagesJournal = "_jms.bridge_junit_messages";
    Map<JournalEntryKey, JournalEntry> table = getCompactedJournal(kafkaContainer, messagesJournal);
    TestSupport.logTable(messagesJournal, table);
    assertEquals(0, table.size(), "Compacted table should be empty.");
  }

  @Test
  @Timeout(30)
  public void jmsClientPubSubMultipleBindings() throws Exception {
    String topicName = "jms-client-multi-bindings";
    String subscriberName = "jms-client-multi-bindings-subscriber";

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

    //wait for streams to commit
    Thread.sleep(1000);

    String messagesJournal = "_jms.bridge_junit_messages";
    Map<JournalEntryKey, JournalEntry> table = getCompactedJournal(kafkaContainer, messagesJournal);
    TestSupport.logTable(messagesJournal, table);
    assertEquals(0, table.size(), "Compacted table should be empty.");
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

  public Stream<Pair<JournalEntryKey, JournalEntry>> streamJournalFiles(String journalTopic) {
    return kafkaContainer
        .consumeAll(journalTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        .stream()
        .map(r -> {

          JournalEntryKey rkey = null;
          if (r.key() != null) {
            try {
              rkey = JournalEntryKey.parseFrom(r.key());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          JournalEntry rval = null;
          if (r.value() != null) {
            try {
              rval = JournalEntry.parseFrom(r.value());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return Pair.of(rkey, rval);
        });
  }

  public void logJournalFiles(String journalTopic) {
    LogFormat format = LogFormat.forSubject("JournalLog");

    String journalStr = streamJournalFiles(journalTopic)
        .map(pair -> format.build(b -> {

          b.addJournalEntryKey(pair.getKey());
          if (pair.getValue() == null) {
            b.event("TOMBSTONE");
          } else {
            b.event("ENTRY");
            b.addJournalEntry(pair.getValue());
          }

        }))
        .collect(Collectors.joining(System.lineSeparator()));

    println(
        "#### JOURNAL FOR TOPIC " + journalTopic + " ####" + System.lineSeparator() + journalStr);
  }
}
