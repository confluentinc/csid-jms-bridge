/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.KafkaContainer;

@Ignore
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ConfluentAmqServerTests {

  static final Serde<String> stringSerde = Serdes.String();

  @ClassRule
  public static KafkaContainer kafkaContainer = new KafkaContainer("5.4.0")
      .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
      .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
  static AdminClient kafkaAdminClient;
  static KafkaProducer<byte[], byte[]> kafkaProducer;
  static ConfluentEmbeddedAmq amqServer;

  @ClassRule
  public static TemporaryFolder amqDataDir = new TemporaryFolder();

  Connection amqConnection;
  List<String> kafkaTopics;

  @BeforeClass
  public static void setupAll() throws Exception {
    Properties kafkaProps = new Properties();
    kafkaProps
        .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "amq-server-test");

    kafkaAdminClient = AdminClient.create(kafkaProps);
    kafkaProducer = new KafkaProducer<>(kafkaProps, new ByteArraySerializer(),
        new ByteArraySerializer());
    amqServer = new ConfluentEmbeddedAmq.Builder(kafkaProps).build();

    Configuration amqConf = amqServer.getAmq().getConfiguration();
    amqConf.setBindingsDirectory(amqDataDir.newFolder("bindings").getAbsolutePath());
    amqConf.setJournalDirectory(amqDataDir.newFolder("journal").getAbsolutePath());
    amqConf.setPagingDirectory(amqDataDir.newFolder("Paging").getAbsolutePath());
    amqConf.setLargeMessagesDirectory(amqDataDir.newFolder("large-messages").getAbsolutePath());
    amqConf
        .setNodeManagerLockDirectory(amqDataDir.newFolder("node-manager-lock").getAbsolutePath());

    amqServer.start();
  }

  @AfterClass
  public static void cleanupAll() throws Exception {
    if (amqServer != null) {
      amqServer.stop();
      amqServer = null;
    }

    if (kafkaProducer != null) {
      kafkaProducer.close();
    }

    if (kafkaAdminClient != null) {
      kafkaAdminClient.close();
    }
  }

  @Before
  public void setup() throws Exception {
    ConnectionFactory cf = ActiveMQJMSClient
        .createConnectionFactory("tcp://localhost:61616", "unit-test");
    amqConnection = cf.createConnection();
    amqConnection.setClientID("test-client-id");
    amqConnection.start();
    kafkaTopics = new LinkedList<>();
  }

  @After
  public void cleanup() throws Exception {
    if (amqConnection != null) {
      amqConnection.close();
    }

    if (!kafkaTopics.isEmpty()) {
      kafkaAdminClient.deleteTopics(kafkaTopics).all().get();

      boolean topicsRemain = true;
      while (topicsRemain) {
        Thread.sleep(50);
        Set<String> knownTopics = kafkaAdminClient.listTopics().names().get();
        topicsRemain = kafkaTopics.stream().anyMatch(knownTopics::contains);
      }
    }
  }

  public void createKafkaTopic(String name, int partitions) throws Exception {
    NewTopic topic = new NewTopic(name, partitions, (short) 1);
    kafkaAdminClient.createTopics(Collections.singletonList(topic)).all().get();
    kafkaTopics.add(name);
  }

  public <K, V> List<ConsumerRecord<K, V>> consumeAllRecords(String topic, Deserializer<K> keydeser,
      Deserializer<V> valuedeser) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keydeser, valuedeser);
    List<TopicPartition> ptList = consumer.partitionsFor(topic).stream()
        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
        .collect(Collectors.toList());
    consumer.assign(ptList);
    consumer.seekToBeginning(ptList);

    List<ConsumerRecord<K, V>> recordList = new LinkedList<>();
    while (true) {
      ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
      if (records.count() < 1) {
        break;
      }
      records.forEach(recordList::add);
    }

    consumer.close();
    return recordList;
  }

  @Test
  public void demo() throws Exception {
    System.out.println("Kafka bootstrap: " + kafkaContainer.getBootstrapServers());
    String topicName = "jms-to-kafka";
    createKafkaTopic(topicName, 1);

    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(topicName);
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    TextMessage message = session.createTextMessage("Hello Kafka");
    message.setJMSCorrelationID("yo-correlate-man");
    //Exceptions in bridges do bubble up to the client.
    producer.send(message);

    try {
      Message received = consumer.receive(100);
      while (true) {
        Thread.sleep(60000);
      }
    } finally {
      producer.close();
      consumer.close();
      session.close();
    }
  }


  @Test
  public void jmsPublishKafkaConsumeTopic() throws Exception {
    String topicName = "jms-to-kafka";
    createKafkaTopic(topicName, 1);

    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(topicName);
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

    List<ConsumerRecord<String, String>> records = consumeAllRecords(topicName,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());
    assertEquals("Hello Kafka", records.get(0).value());
    assertEquals("yo-correlate-man", records.get(0).key());
    assertTrue(records.get(0).headers().toArray().length > 0);

  }

  @Test
  public void kafkaPublishJmsConsumeTopic() throws Exception {
    String topicName = "jms-to-kafka";
    createKafkaTopic(topicName, 1);

    Session session = amqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(topicName);
    MessageConsumer consumer = session.createDurableConsumer(topic, "test-subscriber");

    //allow the consumer to get situated.
    Thread.sleep(1000);
    RecordMetadata meta = kafkaProducer
        .send(new ProducerRecord<>(topicName, "key".getBytes(), "Hello AMQ".getBytes())).get();

    try {
      Message received = consumer.receive(5000);
      assertNotNull(received);
      assertEquals("Hello AMQ", new String(received.getBody(byte[].class)));
    } finally {
      consumer.close();
      session.close();
    }

    List<ConsumerRecord<String, String>> records = consumeAllRecords(topicName,
        stringSerde.deserializer(), stringSerde.deserializer());
    assertEquals(1, records.size());


  }

}
