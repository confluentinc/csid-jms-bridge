/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic.Builder;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.ArtemisTestServer.Factory;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.util.List;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgeToKafkaTests {


  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.5.2")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  private BridgeConfig.Builder baseConfig = BridgeConfigFactory
      .loadConfiguration(Resources.getResource("base-test-config.conf"));

  @Test
  public void testJmsPropertiesArePassedAlong() throws Exception {
    String kafkaCustomerTopic = kafkaContainer.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(kafkaContainer, b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .id("test")
            .routing(new RoutingConfig.Builder()
                .addTopics(new Builder()
                    .addressTemplate("test.${topic}")
                    .match("customer-topic.*")
                    .messageType("text"))
            .build())));

    String customerAddress = "test." + kafkaCustomerTopic;

    try (
        ArtemisTestServer amq = amqf.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerAddress);

      try (MessageProducer producer = session.createProducer(topic)) {

        TestSupport.retry(10, 500, () -> {
          TextMessage message = session.createTextMessage("Message 1");
          message.setJMSReplyTo(topic);
          message.setStringProperty("foo", "bar");
          message.setJMSCorrelationID("FooCorrelationId");
          producer.send(message);

          List<ConsumerRecord<String, String>> kafkaRecords =
              kafkaContainer.consumeStringsUntil(kafkaCustomerTopic, 1);

          StringDeserializer strDeser = new StringDeserializer();
          LongDeserializer longDeser = new LongDeserializer();

          assertTrue(kafkaRecords.size() > 0);
          ConsumerRecord<String, String> record = kafkaRecords.get(0);

          String topicString = "topic://" + topic.getTopicName();
          assertEquals(topicString,
              strDeser.deserialize("", record.headers().lastHeader("jms.JMSReplyTo").value()));
          assertNotNull(longDeser.deserialize("",
              record.headers().lastHeader("jms.JMSMessageID").value()));
          assertNotNull(
              longDeser.deserialize("", record.headers().lastHeader("jms.JMSTimestamp").value()));
          assertEquals("bar",
              strDeser.deserialize("", record.headers().lastHeader("jms.foo").value()));
          assertEquals(topic.getTopicName(),
              strDeser.deserialize("", record.headers().lastHeader("jms.JMSDestination").value()));
          assertEquals("TEXT",
              strDeser.deserialize("", record.headers().lastHeader("jms.JMSType").value()));
        });
      }
    }
  }

  @Test
  public void testKeySelection() throws Exception {
    String kafkaCustomerTopic = kafkaContainer.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(kafkaContainer, b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .id("test")
            .routing(new RoutingConfig.Builder()
                .addTopics(new Builder()
                    .match("customer-topic.*")
                    .messageType("text")
                    .addressTemplate("test.${topic}"))

            .build())));

    String customerAddress = "test." + kafkaCustomerTopic;


    try (
        ArtemisTestServer amq = amqf.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {
      amq.assertAddressAvailable(customerAddress);

      Topic topic = session.createTopic(customerAddress);

      try (MessageProducer producer = session.createProducer(topic)) {

        TestSupport.retry(10, 500, () -> {
          TextMessage message = session.createTextMessage("Message 1");
          message.setJMSCorrelationID("FooCorrelationId");
          producer.send(message);
          List<ConsumerRecord<String, String>> kafkaRecords =
              kafkaContainer.consumeStringsUntil(kafkaCustomerTopic, 1);

          assertEquals(1, kafkaRecords.size());
          assertTrue(kafkaRecords.stream().anyMatch(r -> "FooCorrelationId".equals(r.key())));
        });

      }


    }
  }
}

