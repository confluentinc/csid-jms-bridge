/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.jms.bridge.itests.exchange;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;


import io.confluent.jms.bridge.itests.security.JaasIT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Testcontainers
public class HeadersIT {

    private static final Logger LOGGER = LoggerFactory
        .getLogger(JaasIT.class);

    private static final DockerImageName KAFKA_IMAGE = DockerImageName
        .parse("confluentinc/cp-kafka:6.2.0-3-ubi8");

    private static final DockerImageName JMS_BRIDGE_IMAGE = DockerImageName
        .parse("local.build/confluentinc/jms-bridge-docker:latest");


    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
        .withNetwork(Network.SHARED)
        .withNetworkAliases("kafka")
        .withEmbeddedZookeeper()
        .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");


    @BeforeEach
    void setup() {
      Map<String, Object> props = new HashMap<>();
      props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

      try {
        AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(Arrays.asList(
            new NewTopic("hdr", Optional.of(1), Optional.of((short) 1)))).all().get();
      } catch(Exception e) {
        if (e.getCause() == null || !(e.getCause() instanceof TopicExistsException)) {
          e.printStackTrace();
        }
      }
    }

    @Test
    void kafka_should_be_running() throws Exception {
      assertWithMessage("kafka container is running")
          .that(kafkaContainer.isRunning()).isTrue();
   }

  @Nested
  @TestMethodOrder(OrderAnnotation.class)
  class HeadersPass {

    @Container
    private final GenericContainer<?> jmsBridgeContainer = new GenericContainer<>(
        JMS_BRIDGE_IMAGE)
        .withNetwork(Network.SHARED)
        .withExposedPorts(61616)
        .waitingFor(new LogMessageWaitStrategy().withRegEx(
            ".*KafkaExchangeManager.ProcessRoutingTopicRule>Completed.*kafka[.]hdr.*"))
        .withEnv("LOG4J2_IO_CONFLUENT_AMQ_EXCHANGE", "INFO")
        .withEnv("JMSBRIDGE_ID", "junit-test")
        .withEnv("JMSBRIDGE_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        .withEnv("JMSBRIDGE_JOURNALS_TOPIC_REPLICATION", "1")
        .withEnv("JMSBRIDGE_ROUTING_TOPICS_0_MATCH", "hdr")
        .withEnv("JMSBRIDGE_ROUTING_TOPICS_0_MESSAGE_TYPE", "text")
        .withEnv("JMSBRIDGE_ROUTING_METADATA_REFRESH_MS", "10000")
        .withStartupAttempts(1)
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(HeadersIT.class)))
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("exchange/headers-broker.xml"),
            "/etc/jms-bridge/broker.xml");

    InitialContext context;
    Topic topic;

    @BeforeEach
    void setupContext() throws Exception {
      assertThat(jmsBridgeContainer.isRunning()).isTrue();

      Hashtable<String, String> jndiProps = new Hashtable<>();
      jndiProps.put("java.naming.factory.initial",
          "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      jndiProps.put("java.naming.provider.url",
          String.format("tcp://%s:%s", "localhost", jmsBridgeContainer.getMappedPort(61616)));
      jndiProps.put("topic.topics/kafka.hdr", "kafka.hdr");

      context = new InitialContext(jndiProps);
      topic = (Topic) context.lookup("topics/kafka.hdr");
    }

    @Test
    void propertiesPassedBetweenJmsClients() throws Exception {
      ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");
      try (JMSContext consumerContext = cf.createContext()) {
        consumerContext.setClientID("headers-consumer");
        JMSConsumer consumer = consumerContext.createDurableConsumer(topic, "headers-consumer-01");
        assertThat(consumer.receiveNoWait()).isNull();

        try (JMSContext producerContext = cf.createContext()) {
          JMSProducer producer = producerContext.createProducer();
          TextMessage msg = producerContext.createTextMessage("headers-test");
          msg.setStringProperty("string_property", "string-property-val");
          msg.setIntProperty("int_property", 1);

          producer.send(topic, msg);
        }

        Message msg = consumer.receive(1000);
        assertThat(msg).isNotNull();
        assertThat(msg.getStringProperty("string_property")).isEqualTo("string-property-val");
        assertThat(msg.getIntProperty("int_property")).isEqualTo(1);
      }
    }

    @Test
    void propertiesPassedBetweenJmsKafkaClients() throws Exception {
      ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");
      try (JMSContext consumerContext = cf.createContext()) {
        consumerContext.setClientID("headers-consumer");
        JMSConsumer consumer = consumerContext.createDurableConsumer(topic, "headers-consumer-01");
        assertThat(consumer.receiveNoWait()).isNull();

        StringSerializer stringSerializer = new StringSerializer();
        LongSerializer longSerializer = new LongSerializer();
        IntegerSerializer integerSerializer = new IntegerSerializer();

        Map<String, Object> kprops = new HashMap<>();
        kprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (KafkaProducer<String, String> kproducer =
            new KafkaProducer<>(kprops, stringSerializer, stringSerializer)) {

          ProducerRecord<String, String> krecord = new ProducerRecord<>("hdr", "key", "value");
          krecord.headers().add(
              "jms.string.string_property",
              stringSerializer.serialize("hdr", "string-property-val"));
          krecord.headers().add(
              "jms.long.long_property",
              longSerializer.serialize("hdr", Long.MAX_VALUE));
          krecord.headers().add(
              "jms.int.integer_property",
              integerSerializer.serialize("hdr", Integer.MAX_VALUE));
          kproducer.send(krecord).get();
        }

        Message msg = consumer.receive(1000);
        assertThat(msg).isNotNull();
        assertThat(msg.getStringProperty("string_property")).isEqualTo("string-property-val");
        assertThat(msg.getIntProperty("integer_property")).isEqualTo(Integer.MAX_VALUE);
        assertThat(msg.getLongProperty("long_property")).isEqualTo(Long.MAX_VALUE);

      }
    }

    void publishMessage(String txtMsg, Topic topic, String user, String pass)
        throws NamingException {

      ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");
      JMSContext jmsContext = cf.createContext(user, pass);
      JMSProducer producer = jmsContext.createProducer();
      producer.send(topic, jmsContext.createTextMessage(txtMsg));
    }

    Connection createJmsConnection(Context context, String username, String password) {
      try {

        String clientId = "headers-test";
        ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");

        int maxTry = 5;
        int attempts = 0;
        Connection cnxn = null;
        while (cnxn == null) {
          try {
            cnxn = cf.createConnection(username, password);
            cnxn.setClientID(clientId);
          } catch (Exception e) {
            if (attempts >= maxTry) {
              throw e;
            }
            attempts++;
            Thread.sleep(1000);
          }
        }

        return cnxn;

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }
}
