/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic.Builder;
import io.confluent.amq.test.AbstractContainerTest;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.ArtemisTestServer.Factory;
import io.confluent.amq.test.KafkaContainerHelper;
import io.confluent.amq.test.TestSupport;
import io.confluent.csid.common.utils.accelerator.Accelerator;
import io.confluent.csid.common.utils.accelerator.ClientId;
import io.confluent.csid.common.utils.accelerator.Owner;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Ignore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.*;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

  @Disabled("Fails in CI for some reason")
  @Test
  public void testSingleMessageForJmsAndKafka_OriginJms() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    consumerHelper.consumeBegin(kafkaCustomerTopic);

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

      try (MessageProducer producer = session.createProducer(topic);
           MessageConsumer consumer = session.createConsumer(topic)) {

        TextMessage message = session.createTextMessage(messagePayload);
        producer.send(message);

        ConsumerRecord<String, String> krecord = consumerHelper.lookUntil(m -> true, Duration.ofSeconds(5));

        assertNotNull(krecord);
        assertEquals(messagePayload, krecord.value());

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

        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        ConsumerRecord<String, String> kafkaRecord =
            consumerHelper.lookUntil(r -> "FooCorrelationId".equals(r.key()));
        assertNotNull(kafkaRecord);

      }
    }
  }
  @Test
  public void testKafkaClientIdsAreCorrect() throws Exception {
    String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
    Factory amqf = ArtemisTestServer.factory();
    String bridgeId = "client_test_bridge";
    String sfcdId = "partner_sfdc_id";
    amqf.prepare(bridgeKafkaProps, b -> b
            .mutateJmsBridgeConfig(bridge -> bridge
                    .mergeFrom(baseConfig)
                    .id("test")
                    .clientId(new BridgeClientId.Builder()
                            .bridgeId(bridgeId)
                            .partnerSFDCId(sfcdId))
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

      try (MessageProducer producer = session.createProducer(topic)) {

        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSReplyTo(topic);
        message.setStringProperty("foo", "bar");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        session.commit();

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Set<ObjectInstance> objectInstanceSet = mBeanServer.queryMBeans(null, null);
        Long bridgeCount = objectInstanceSet.stream()
                .map(ObjectInstance::toString)
                .filter(n -> n.contains(bridgeId))
                .count();

        Long sfdcCount = objectInstanceSet.stream()
                .map(ObjectInstance::toString)
                .filter(n -> n.contains(sfcdId))
                .count();

        Long accIdCount = objectInstanceSet.stream()
                .map(ObjectInstance::toString)
                .filter(n -> n.contains(Accelerator.JMS_BRIDGE.getAcceleratorId()))
                .count();

        Long ownerIdCount = objectInstanceSet.stream()
                .map(ObjectInstance::toString)
                .filter(n -> n.contains(Owner.PIE_LABS.name()))
                .count();

        assertTrue(bridgeCount <= sfdcCount, "bridge client ids should include partner sfdc id");
        assertTrue(bridgeCount <=  accIdCount, "bridge client ids should include accelerator id");
        assertTrue(bridgeCount <= ownerIdCount, "bridge client ids should include owner id");
      }
    }
  }
}

