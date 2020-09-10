/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static io.confluent.amq.SerdePool.deserString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import java.util.List;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings({"MS_SHOULD_BE_FINAL", "MS_PKGPROTECT"})
@Disabled("Functionality currently not available.")
@Tag("IntegrationTest")
public class ConfluentAmqServerTests {

  private static final boolean IS_VANILLA = false;
  private static final String JMS_TOPIC = "jms-to-kafka";
  static final Serde<String> stringSerde = Serdes.String();

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
      .jmsBridgeConfigBuilder()
        .putAllKafka(BridgeConfigFactory.propsToMap(kafkaContainer.defaultProps())));

  @Test
  public void jmsPublishKafkaConsumeTopic() throws Exception {
    System.out.println(">>>>> IS VANILLA IS " + IS_VANILLA + " <<<<<<<");
    Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    TextMessage message = session.createTextMessage("Hello Kafka");
    message.setJMSCorrelationID("yo-correlate-man");
    //Exceptions in bridges do bubble up to the client.
    producer.send(message);

    Thread.sleep(1000);

    try {
      Message received = consumer.receive(100);
      assertEquals("Hello Kafka", received.getBody(String.class));
    } finally {
      producer.close();
      consumer.close();
      session.close();
    }

    List<ConsumerRecord<String, String>> records = kafkaContainer.consumeAll(JMS_TOPIC,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());
    assertEquals("Hello Kafka", records.get(0).value());
    assertEquals("yo-correlate-man", records.get(0).key());
    assertTrue(records.get(0).headers().toArray().length > 0);

  }

  @Test
  public void kafkaPublishJmsConsumeTopic() throws Exception {
    Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(JMS_TOPIC);
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    //allow the consumer to get situated.
    Thread.sleep(1000);
    kafkaContainer.publish(JMS_TOPIC, "key", "Hello AMQ");

    try {
      Message received = consumer.receive(5000);
      assertNotNull(received);
      assertEquals("Hello AMQ",
          deserString(received.getBody(byte[].class)));
    } finally {
      consumer.close();
      session.close();
    }

    List<ConsumerRecord<String, String>> records = kafkaContainer.consumeAll(JMS_TOPIC,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());
  }



}
