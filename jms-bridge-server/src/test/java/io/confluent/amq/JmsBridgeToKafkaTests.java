/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeConfig.JournalConfig;
import io.confluent.amq.config.BridgeConfig.JournalsConfig;
import io.confluent.amq.config.BridgeConfig.TopicConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig.Convert;
import io.confluent.amq.config.RoutingConfig.In;
import io.confluent.amq.config.RoutingConfig.Out;
import io.confluent.amq.config.RoutingConfig.Route.Builder;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
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

  static String customerQueue = "customer.queue";
  static String kafkaCustomerTopic = "customer.update";

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
      .jmsBridgeConfigBuilder()
      .id("test-bridge")
      .journals(new JournalsConfig.Builder()
          .maxMessageSize(1024 * 1024)
          .readyTimeout(Duration.ofSeconds(5))
          .readyCheckInterval(Duration.ofSeconds(1))
          .topic(new TopicConfig.Builder()
              .partitions(1)
              .replication(1))
          .messages(new JournalConfig.Builder()
              .topic(new TopicConfig.Builder()
                  .partitions(1)
                  .replication(1)))
          .bindings(new JournalConfig.Builder()
              .topic(new TopicConfig.Builder()
                  .partitions(1)
                  .replication(1))))
      .putAllKafka(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps()))
      .putStreams(StreamsConfig.STATE_DIR_CONFIG, tempdir.toAbsolutePath().toString())
      .putAllStreams(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps()))
      .routingBuilder()
      .addRoutes(new Builder()
          .name("test-route")
          .from(new In.Builder()
              .address(customerQueue))
          .map(new Convert.Builder())
          .to(new Out.Builder()
              .topic(kafkaCustomerTopic))));


  @Test
  @Timeout(30)
  public void jmsMessageToKafkaTopic() throws Exception {
    String subscriberName = "jms-message-to-kafka-subscriber";
    kafkaContainer.createTempTopic(kafkaCustomerTopic, 1);

    try (
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
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

    }
    //wait for streams to commit
    Thread.sleep(500);
    List<ConsumerRecord<byte[], String>> kafkaRecords = kafkaContainer.consumeAll(
        kafkaCustomerTopic, new ByteArrayDeserializer(), new StringDeserializer());

    assertEquals(2, kafkaRecords.size());
    kafkaRecords.forEach(r -> TestSupport.println(r.toString()));
  }
}

