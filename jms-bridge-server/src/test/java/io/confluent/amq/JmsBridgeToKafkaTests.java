/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic.Builder;
import io.confluent.amq.test.AbstractContainerTest;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.ArtemisTestServer.Factory;
import io.confluent.amq.test.KafkaContainerHelper;
import io.confluent.amq.test.TestSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgeToKafkaTests extends AbstractContainerTest {

  public KafkaContainerHelper.AdminHelper adminHelper = getContainerHelper().adminHelper();

  public KafkaContainerHelper.ConsumerHelper<String, String> consumerHelper;

  public KafkaContainerHelper.ProducerHelper<String, String> producerHelper;

  public Properties bridgeKafkaProps;

  public BridgeConfig.Builder baseConfig = BridgeConfigFactory
      .loadConfiguration(Resources.getResource("base-test-config.conf"));

  @TempDir
  @Order(100)
  public static Path tempdir;

  @BeforeEach
  public void setup() throws Exception {
    consumerHelper = getContainerHelper().consumerHelper(
        TestSupport.stringDeserializer(),
        TestSupport.stringDeserializer());

    producerHelper = getContainerHelper().producerHelper(
        TestSupport.stringSerializer(),
        TestSupport.stringSerializer()
    );
    bridgeKafkaProps = new Properties();
    bridgeKafkaProps.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getContainerHelper().bootstrapServers());
  }

  @Test
  public void testJmsPropertiesArePassedAlong() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(bridgeKafkaProps, b -> b
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
        Session session = amq.getConnection().createSession(true, Session.SESSION_TRANSACTED)
    ) {

      Topic topic = session.createTopic(customerAddress);
      consumerHelper.consumeBegin(kafkaCustomerTopic);

      try (MessageProducer producer = session.createProducer(topic)) {

        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSReplyTo(topic);
        message.setStringProperty("foo", "bar");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        session.commit();

        String topicString = "topic://" + topic.getTopicName();
        StringDeserializer strDeser = new StringDeserializer();
        LongDeserializer longDeser = new LongDeserializer();

        consumerHelper.lookUntil(record -> {

          assertEquals(topicString,
              strDeser
                  .deserialize("", record.headers().lastHeader("jms.string.JMSReplyTo").value()));
          assertNotNull(longDeser.deserialize("",
              record.headers().lastHeader("jms.long.JMSMessageID").value()));
          assertNotNull(
              longDeser
                  .deserialize("", record.headers().lastHeader("jms.long.JMSTimestamp").value()));
          assertEquals("bar",
              strDeser.deserialize("", record.headers().lastHeader("jms.string.foo").value()));
          assertEquals("TEXT",
              strDeser.deserialize("", record.headers().lastHeader("jms.string.JMSType").value()));
          return true;
        });
      }
    }
  }

  @Test
  public void testSingleMessageForJmsAndKafka_OriginKafka() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(bridgeKafkaProps, b -> b
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
    String messagePayload = "Message Payload";

    try (
        ArtemisTestServer amq = amqf.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {
      amq.assertAddressAvailable(customerAddress);

      Topic topic = session.createTopic(customerAddress);
      consumerHelper.consumeBegin(kafkaCustomerTopic);

      try (MessageConsumer consumer = session.createConsumer(topic)) {


        int attempts = 0;
        while (attempts < 10
            && !amq.confluentAmqServer()
            .getKafkaExchangeManager()
            .currentSubscribedKafkaTopics()
            .contains(kafkaCustomerTopic)) {

          Thread.sleep(100);
          attempts++;
        }

        producerHelper.publish(kafkaCustomerTopic, "key", messagePayload);


        ConsumerRecord<String, String> krecord = consumerHelper.lookUntil(record ->
            messagePayload.equals(record.value()));
        assertNotNull(krecord);

        Message jmsMessage = consumer.receive(1000);
        Message jmsMessage2 = consumer.receive(1000);

        assertNull(jmsMessage2);
        assertNotNull(jmsMessage);
        assertEquals(messagePayload, jmsMessage.getBody(String.class));

      }


    }
  }

  @Test
  public void testSingleMessageForJmsAndKafka_OriginJms() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(bridgeKafkaProps, b -> b
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
    String messagePayload = "Message Payload";

    try (
        ArtemisTestServer amq = amqf.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {
      amq.assertAddressAvailable(customerAddress);

      Topic topic = session.createTopic(customerAddress);
      consumerHelper.consumeBegin(kafkaCustomerTopic);

      try (MessageProducer producer = session.createProducer(topic);
           MessageConsumer consumer = session.createConsumer(topic)) {

        TextMessage message = session.createTextMessage(messagePayload);
        producer.send(message);

        ConsumerRecord<String, String> krecord = consumerHelper.lookUntil(record ->
            messagePayload.equals(record.value()));
        assertNotNull(krecord);

        Message jmsMessage = consumer.receive(1000);
        Message jmsMessage2 = consumer.receive(1000);

        assertNull(jmsMessage2);
        assertEquals(messagePayload, jmsMessage.getBody(String.class));
        assertNotNull(jmsMessage);

      }


    }
  }

  @Test
  public void testKeySelection() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(bridgeKafkaProps, b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .id("test")
            .routing(new RoutingConfig.Builder()
                .putAllProducer(baseConfig.kafka())
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
      consumerHelper.consumeBegin(kafkaCustomerTopic);

      try (MessageProducer producer = session.createProducer(topic)) {

        StringDeserializer stringDeserializer = TestSupport.stringDeserializer();
        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        ConsumerRecord<String, String> kafkaRecord =
            consumerHelper.lookUntil(r -> "FooCorrelationId".equals(r.key()));
        assertNotNull(kafkaRecord);

      }


    }
  }
}

