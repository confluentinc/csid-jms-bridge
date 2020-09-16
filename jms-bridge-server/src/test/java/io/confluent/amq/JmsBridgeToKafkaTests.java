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
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  private static final AtomicInteger TOPIC_SEQ = new AtomicInteger(1);

  private BridgeConfig.Builder baseConfig;

  private String customerQueue;
  private String kafkaCustomerTopic;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @BeforeEach
  public void before() {
    int testSeq = TOPIC_SEQ.getAndIncrement();
    customerQueue = "customer.queue." + testSeq;
    kafkaCustomerTopic = "customer.update." + testSeq;

    File stateDir = tempdir.resolve("streams-state-test-" + testSeq).toFile();
    stateDir.mkdir();
    baseConfig = BridgeConfigFactory.loadConfiguration(Resources
        .getResource("base-test-config.conf"))
        .id("test-bridge-" + TOPIC_SEQ.getAndIncrement())
        .putAllKafka(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps()))
        .putAllStreams(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps()))
        .putStreams(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
  }

  @Test
  @Timeout(30)
  public void jmsMessageToKafkaTopic() throws Exception {
    String subscriberName = "jms-message-to-kafka-subscriber";
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt
                    .name("test-route")
                    .mutateFrom(f -> f
                        .address(customerQueue))
                    .mutateTo(to -> to
                        .topic(kafkaCustomerTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        producer.send(session.createTextMessage("Message 1"));
        Message rcvmsg = consumer.receive(100);
        assertEquals("Message 1", rcvmsg.getBody(String.class));

        TextMessage message = session.createTextMessage("Message 2");
        message.setJMSMessageID("ID:jmsMessageIdFoo");
        message.setJMSCorrelationID("CorrelationJmsMessageIdFoo");
        producer.send(message);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 2", rcvmsg.getBody(String.class));

      }

      List<ConsumerRecord<byte[], String>> kafkaRecords = kafkaContainer.consumeBytesStringsUntil(
          kafkaCustomerTopic, 2);

      assertEquals(2, kafkaRecords.size());
      kafkaRecords.forEach(r -> TestSupport.println(r.toString()));

    }
  }

  @Test
  public void testJmsPropertiesArePassedAlong() throws Exception {
    String subscriberName = "jms-to-kafka-properties-are-passed-subscriber";
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt.name("test-route1")
                    .mutateFrom(f -> f.address(customerQueue))
                    .mutateTo(to -> to.topic(kafkaCustomerTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSReplyTo(topic);
        message.setStringProperty("foo", "bar");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        Message rcvmsg = consumer.receive(100);
        assertEquals("Message 1", rcvmsg.getBody(String.class));
        assertEquals(topic, rcvmsg.getJMSDestination());
        assertEquals(topic, rcvmsg.getJMSReplyTo());
        assertEquals("bar", rcvmsg.getStringProperty("foo"));

      }

      List<ConsumerRecord<String, String>> kafkaRecords =
          kafkaContainer.consumeStringsUntil(kafkaCustomerTopic, 1);

      StringDeserializer strDeser = new StringDeserializer();
      LongDeserializer longDeser = new LongDeserializer();

      assertEquals(1, kafkaRecords.size());
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
      assertEquals("text",
          strDeser.deserialize("", record.headers().lastHeader("jms.JMSType").value()));
    }
  }

  @Test
  public void testKeySelection() throws Exception {
    String subscriberName = "jms-to-kafka-key-selection-subscriber";
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt.name("test-route1")
                    .mutateFrom(f -> f.address(customerQueue))
                    .mutateMap(m -> m.key("JMSCorrelationID"))
                    .mutateTo(to -> to.topic(kafkaCustomerTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        TextMessage message = session.createTextMessage("Message 1");
        message.setJMSCorrelationID("FooCorrelationId");
        producer.send(message);
        Message rcvmsg = consumer.receive(100);
        assertEquals("Message 1", rcvmsg.getBody(String.class));
        assertEquals("FooCorrelationId", rcvmsg.getJMSCorrelationID());

      }

      List<ConsumerRecord<String, String>> kafkaRecords =
          kafkaContainer.consumeStringsUntil(kafkaCustomerTopic, 1);

      assertEquals(1, kafkaRecords.size());
      assertTrue(kafkaRecords.stream().anyMatch(r -> "FooCorrelationId".equals(r.key())));

    }
  }

  @Test
  public void testMultiRouteSameAddress() throws Exception {
    String subscriberName = "jms-to-kafka-multi-route-one-address-subscriber";
    String otherKafkaTopic = "other-kafka-topic-" + TOPIC_SEQ.getAndIncrement();
    kafkaContainer.createTempTopic(otherKafkaTopic, 1);
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt.name("test-route1")
                    .mutateFrom(f -> f.address(customerQueue))
                    .mutateTo(to -> to.topic(kafkaCustomerTopic)))
                .addRoute(rt -> rt.name("test-route2")
                    .mutateFrom(f -> f.address(customerQueue))
                    .mutateTo(to -> to.topic(otherKafkaTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        producer.send(session.createTextMessage("Message 1"));
        Message rcvmsg = consumer.receive(100);
        assertEquals("Message 1", rcvmsg.getBody(String.class));

        TextMessage message = session.createTextMessage("Message 2");
        message.setJMSMessageID("ID:jmsMessageIdFoo");
        message.setJMSCorrelationID("CorrelationJmsMessageIdFoo");
        producer.send(message);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 2", rcvmsg.getBody(String.class));

      }

      List<ConsumerRecord<byte[], String>> kafkaRecords =
          kafkaContainer.consumeBytesStringsUntil(kafkaCustomerTopic, 2);
      assertEquals(2, kafkaRecords.size());

      kafkaRecords =
          kafkaContainer.consumeUntil(
              otherKafkaTopic,
              new ByteArrayDeserializer(),
              new StringDeserializer(),
              1,
              Duration.ofSeconds(1));

      assertEquals(0, kafkaRecords.size());
    }
  }

  @Test
  public void testSingleRouteWithFilter() throws Exception {
    String subscriberName = "jms-to-kafka-route-with-filter-subscriber";
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt.name("test-route1")
                    .mutateFrom(f -> f
                        .address(customerQueue)
                        .filter("classification <> 'secret'"))
                    .mutateTo(to -> to.topic(kafkaCustomerTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        TextMessage message1 = session.createTextMessage("Secret Message 1");
        message1.setStringProperty("classification", "secret");
        producer.send(message1);
        Message rcvmsg = consumer.receive(100);
        assertEquals("Secret Message 1", rcvmsg.getBody(String.class));

        TextMessage message2 = session.createTextMessage("Message 2");
        message2.setStringProperty("classification", "none");
        producer.send(message2);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 2", rcvmsg.getBody(String.class));

        TextMessage message3 = session.createTextMessage("Message 3");
        producer.send(message3);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 3", rcvmsg.getBody(String.class));
      }

      List<ConsumerRecord<byte[], String>> kafkaRecords = kafkaContainer.consumeBytesStringsUntil(
          kafkaCustomerTopic, 2);

      assertEquals(2, kafkaRecords.size());
      assertTrue(kafkaRecords.stream().anyMatch(r -> !"Secret Message 1".equals(r.value())));
    }
  }

  @Test
  public void testMultiRouteSameAddressDifferentFilter() throws Exception {
    String subscriberName = "jms-to-kafka-multi-route-with-multi-filter-subscriber";
    String secretTopic = "secret-kafka-topic";

    kafkaContainer.createTempTopic(secretTopic, 1);
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);
    ArtemisTestServer amqServer = ArtemisTestServer.embedded(b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .mutateRouting(routing -> routing
                .addRoute(rt -> rt.name("test-not-secret")
                    .mutateFrom(f -> f
                        .address(customerQueue)
                        .filter("classification <> 'secret'"))
                    .mutateTo(to -> to.topic(kafkaCustomerTopic)))
                .addRoute(rt -> rt.name("test-secret")
                    .mutateFrom(f -> f
                        .address(customerQueue)
                        .filter("classification = 'secret'"))
                    .mutateTo(to -> to.topic(secretTopic))))));

    try (
        ArtemisTestServer amq = amqServer.start();
        Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session.createTopic(customerQueue);

      try (
          MessageProducer producer = session.createProducer(topic);
          MessageConsumer consumer = session.createDurableConsumer(topic, subscriberName)
      ) {

        TextMessage message1 = session.createTextMessage("Secret Message 1");
        message1.setStringProperty("classification", "secret");
        producer.send(message1);
        Message rcvmsg = consumer.receive(100);
        assertEquals("Secret Message 1", rcvmsg.getBody(String.class));

        TextMessage message2 = session.createTextMessage("Message 2");
        message2.setStringProperty("classification", "none");
        producer.send(message2);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 2", rcvmsg.getBody(String.class));

        TextMessage message3 = session.createTextMessage("Message 3");
        producer.send(message3);
        rcvmsg = consumer.receive(100);
        assertEquals("Message 3", rcvmsg.getBody(String.class));
      }

      List<ConsumerRecord<byte[], String>> kafkaRecords = kafkaContainer.consumeBytesStringsUntil(
          kafkaCustomerTopic, 2);

      assertEquals(2, kafkaRecords.size());
      assertTrue(kafkaRecords.stream().anyMatch(r -> !"Secret Message 1".equals(r.value())));

      kafkaRecords = kafkaContainer.consumeBytesStringsUntil(
          secretTopic, 1);

      assertEquals(1, kafkaRecords.size());
      assertTrue(kafkaRecords.stream().anyMatch(r -> "Secret Message 1".equals(r.value())));

    }
  }
}

