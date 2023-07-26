/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.jms.bridge.itests.security;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.Hashtable;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class JaasTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(JaasTest.class);

  private static final DockerImageName KAFKA_IMAGE = DockerImageName
      .parse("confluentinc/cp-kafka:7.3.2-1-ubi8");

  private static final DockerImageName JMS_BRIDGE_IMAGE = DockerImageName
      .parse("local.build/confluentinc/jms-bridge-docker:latest");


  @Container
  private static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE)
      .withNetwork(Network.SHARED)
      .withNetworkAliases("kafka")
      .withEmbeddedZookeeper()
      .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
      .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  @Container
  private static final GenericContainer<?> jmsBridgeContainer = new GenericContainer<>(
      JMS_BRIDGE_IMAGE)
      .withNetwork(Network.SHARED)
      .withExposedPorts(61616)
      .withEnv("LOGROOT", "WARN")
      .withEnv("LOG_ORG_APACHE_ACTIVEMQ", "DEBUG")
      .withEnv("JMSBRIDGE_ID", "junit-test")
      .withEnv("JMSBRIDGE_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
      .withEnv("JMSBRIDGE_SECURITY_DOMAIN", "TestDomain")
      .withEnv("JMSBRIDGE_JOURNALS_TOPIC_REPLICATION", "1")
      .withStartupAttempts(1)
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(JaasTest.class)))
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("security/jaas/broker.xml"),
          "/etc/jms-bridge/broker.xml")
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("security/jaas/roles.properties"),
          "/etc/jms-bridge/test_roles.properties")
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("security/jaas/users.properties"),
          "/etc/jms-bridge/test_users.properties")
      .withCopyFileToContainer(
          MountableFile.forClasspathResource("security/jaas/login.config"),
          "/etc/jms-bridge/login.config");

  @Test
  void kafka_should_be_running() {
    assertWithMessage("kafka container is running")
        .that(kafkaContainer.isRunning()).isTrue();
  }

  @Nested
  @TestMethodOrder(OrderAnnotation.class)
  class SecurityAppliedTest {

    InitialContext context;
    Topic topic;
    Queue queue;

    @BeforeEach
    void setupContext() throws Exception {
      Hashtable<String, String> jndiProps = new Hashtable<>();
      jndiProps.put("java.naming.factory.initial",
          "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      jndiProps.put("java.naming.provider.url",
          String.format("tcp://%s:%s", "localhost", jmsBridgeContainer.getMappedPort(61616)));
      jndiProps.put("topic.topics/my-topic.1", "my-topic.1");
      jndiProps.put("queue.queues/my-queue1", "my-queue1");

      context = new InitialContext(jndiProps);
      queue = (Queue) context.lookup("queues/my-queue1");
      topic = (Topic) context.lookup("topics/my-topic.1");
    }

    @Test
    @Order(10)
    void consumerCanCreateDurableQueue() throws Exception {
      assertThat(queue).isNotNull();
      Message msg = consumeMessage(queue, "theConsumer", "theConsumer123");
      assertThat(msg).isNull();
    }

    @Test
    @Order(20)
    void publisherCanPublishToTopic() throws Exception {
      assertDoesNotThrow(()
          -> publishMessage("test message", topic, "theProducer", "theProducer123"));
    }

    @Test
    @Order(25)
    void consumerCanReceiveMessage() throws Exception {
      Message msg = consumeMessage(queue, "theConsumer", "theConsumer123");
      assertThat(msg).isNotNull();
      assertThat(msg.getBody(String.class)).isEqualTo("test message");

    }


    @Test
    @Order(30)
    void publisherCannotConsumeFromQueue() throws Exception {
      assertThat(queue).isNotNull();
      JMSSecurityRuntimeException e = assertThrows(JMSSecurityRuntimeException.class, () ->
          consumeMessage(queue, "theProducer", "theProducer123"));
      assertThat(e).isNotNull();
    }

    @Test
    @Order(40)
    void consumerCannotPublishToTopic() throws Exception {
      assertThat(queue).isNotNull();
      JMSSecurityRuntimeException e = assertThrows(JMSSecurityRuntimeException.class, () ->
          publishMessage("error error", topic, "theConsumer", "theConsumer123"));
      assertThat(e).isNotNull();
    }

    Message consumeMessage(Queue queue, String user, String pass)
        throws JMSException, NamingException {
      Message result;
      ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");
      JMSContext jmsContext = cf.createContext(user, pass);
      try (JMSConsumer consumer = jmsContext.createSharedDurableConsumer(topic, queue.getQueueName())) {
        result = consumer.receiveNoWait();
      }
      return result;
    }

    void publishMessage(String txtMsg, Topic topic, String user, String pass)
        throws NamingException {

      ConnectionFactory cf = (ConnectionFactory) context.lookup("ConnectionFactory");
      JMSContext jmsContext = cf.createContext(user, pass);
      JMSProducer producer = jmsContext.createProducer();
      producer.send(topic, jmsContext.createTextMessage(txtMsg));
    }

    private Connection createJmsConnection(Context context, String username, String password) {
      try {

        String clientId = "jaas-test";
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
