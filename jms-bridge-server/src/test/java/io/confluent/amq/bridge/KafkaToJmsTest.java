/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.exchange.KafkaExchangeUtil;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings("MS_SHOULD_BE_FINAL")
public class KafkaToJmsTest {

  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.5.2")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  @RegisterExtension
  @Order(300)
  public static final ArtemisTestServer amqServer = ArtemisTestServer
      .embedded(kafkaContainer, b -> b
          .mutateJmsBridgeConfig(br -> br
              .putKafka(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
              .routing(new RoutingConfig.Builder()
                  .addTopics(new RoutedTopic.Builder()
                      .messageType("TEXT")
                      .match("herring.*")
                      .addressTemplate("test.${topic}"))
                  .build())
          )
          .dataDirectory(tempdir.toAbsolutePath().toString()));


  @Test
  public void testKafkaTopicAddressIsAvailable() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);
  }

  @Test
  public void testConsumerNotConsumingForUnboundAddress() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();
    amqServer.assertAddressAvailable("test." + herringTopic);

    kafkaContainer.publish(herringTopic, "key", "value");
    kafkaContainer.publish(herringTopic, "key", "value");

    String pluginConsumerGroup = KafkaExchangeUtil.createConsumerGroupId(
        amqServer.confluentAmqServer().getBridgeConfig());

    boolean isConsumingTopic = kafkaContainer.adminClient()
        .listConsumerGroupOffsets(pluginConsumerGroup)
        .partitionsToOffsetAndMetadata()
        .get()
        .keySet()
        .stream()
        .anyMatch(tp -> tp.topic().equals(herringTopic));

    assertFalse(isConsumingTopic, "Consumer should not be consuming unbound topic address");
  }

  @Test
  public void testConsumerIsConsumingForBoundAddress() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    String subscriberName = amqServer.safeId("subscriber-name");
    try (
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(herringAddress);

      try (
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        TestSupport.retry(3, 1000, () -> {
          kafkaContainer.publish(herringTopic, "key", "value");
          Message rcvmsg = consumer.receive(100);
          assertNotNull(rcvmsg);
          assertEquals("value", rcvmsg.getBody(String.class));
        });

      }
    }
  }

  @Test
  public void testPublishViaJmsConsumeFromKafka() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    try (Session session = amqServer.getConnection()
        .createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      Topic herringDest = session.createTopic(herringAddress);

      try (MessageProducer producer = session.createProducer(herringDest)) {
        producer.send(session.createTextMessage("hey kafka"));

        List<ConsumerRecord<byte[], String>> records =
            kafkaContainer.consumeBytesStringsUntil(herringTopic, 1);

        assertEquals(1, records.size());
        assertEquals("hey kafka", records.get(0).value());
      }
    }
  }

  @Test
  public void testConsumerReceivesJmsOriginatedKafkaMessage() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    try (Session session = amqServer.getConnection()
        .createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      Topic herringDest = session.createTopic(herringAddress);

      try (MessageProducer producer = session.createProducer(herringDest);
          MessageConsumer consumer = session.createConsumer(herringDest)) {

        TestSupport.retry(30, 100, () ->
            assertTrue(amqServer.confluentAmqServer()
                .getKafkaExchangeManager()
                .currentSubscribedKafkaTopics()
                .contains(herringTopic)));

        producer.send(session.createTextMessage("hey kafka"));

        List<ConsumerRecord<byte[], String>> records =
            kafkaContainer.consumeBytesStringsUntil(herringTopic, 1);

        assertEquals(1, records.size());
        assertEquals("hey kafka", records.get(0).value());

        Message rcvmsg = consumer.receive(30_000);
        assertNotNull(rcvmsg);
        assertEquals("hey kafka", rcvmsg.getBody(String.class));
      }
    }
  }

  @Test
  public void testPublishViaKafkaConsumeFromJms() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();
    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    try (Session session = amqServer.getConnection()
        .createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      Topic herringDest = session.createTopic(herringAddress);

      try (MessageConsumer consumer = session.createConsumer(herringDest)) {

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herringTopic, "key", "value");
          Message rcvmsg = consumer.receive(100);
          assertNotNull(rcvmsg);
          assertEquals("value", rcvmsg.getBody(String.class));
        });
      }
    }
  }

  @Test
  public void testExchangeIsRemovedWhenTopicIsDeleted_noConsumers() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    try (Session session = amqServer.getConnection()
        .createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      Topic herringDest = session.createTopic(herringAddress);

      try (MessageConsumer consumer = session.createConsumer(herringDest)) {

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herringTopic, "key", "value");
          Message rcvmsg = consumer.receive(100);
          assertNotNull(rcvmsg);
        });

      }

      kafkaContainer.deleteTopics(herringTopic);

      TestSupport.retry(10, 500, () -> {
        amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();
        assertFalse(
            Arrays.asList(amqServer.serverControl().getAddressNames()).contains(herringAddress));
      });
    }
  }


  @Test
  @Disabled("Sporadically fails on Jenkins, works in the IDE")
  public void testExchangeIsRemovedWhenTopicIsDeletedBridgeGracefullyMovesOn() throws Exception {
    String herringTopic = kafkaContainer.safeCreateTopic("herring-events", 3);
    String herring2Topic = kafkaContainer.safeCreateTopic("herring2-events", 3);
    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String herringAddress = "test." + herringTopic;
    amqServer.assertAddressAvailable(herringAddress);

    String herring2Address = "test." + herring2Topic;
    amqServer.assertAddressAvailable(herring2Address);

    try (Session session = amqServer.getConnection()
        .createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      Topic herringDest = session.createTopic(herringAddress);
      Topic herring2Dest = session.createTopic(herring2Address);

      try (MessageConsumer consumer = session.createConsumer(herringDest)) {

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herringTopic, "key", "value");
          Message rcvmsg = consumer.receive(1000);
          assertNotNull(rcvmsg);
        });

      }

      try (MessageConsumer consumer = session.createConsumer(herring2Dest)) {

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herring2Topic, "key", "value");
          Message rcvmsg = consumer.receive(100);
          assertNotNull(rcvmsg);
        });

        while (true) {
          if (consumer.receive(100) == null) {
            break;
          }
        }

        kafkaContainer.deleteTopics(herringTopic);

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herring2Topic, "key", "value2");
          Message rcvmsg = consumer.receive(1000);
          assertNotNull(rcvmsg);
          assertEquals("value2", rcvmsg.getBody(String.class));
        });

        while (true) {
          if (consumer.receive(100) == null) {
            break;
          }
        }

        amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

        TestSupport.retry(10, 1000, () -> {
          kafkaContainer.publish(herring2Topic, "key", "value3");
          Message rcvmsg = consumer.receive(1000);
          assertNotNull(rcvmsg);
          assertEquals("value3", rcvmsg.getBody(String.class));
        });
      }

      TestSupport.retry(3, 1000, () -> {
        assertFalse(
            Arrays.asList(amqServer.serverControl().getAddressNames()).contains(herringAddress));
      });
    }
  }
}
