package io.confluent.amq;

import com.google.common.io.Resources;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.csid.common.test.utils.containers.KafkaKraftCluster;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static io.confluent.amq.test.AbstractContainerTest.getContainerHelper;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Slf4j
@Category(IntegrationTest.class)
public class ChaosKafkaClusterIT {

    public Properties bridgeKafkaProps;
    public BridgeConfig.Builder baseConfig = BridgeConfigFactory
            .loadConfiguration(Resources.getResource("base-test-config.conf"));
    KafkaKraftCluster kafkaKraftCluster;
    ArtemisTestServer.Factory amqf = ArtemisTestServer.factory();

    ArtemisTestServer amq;
    Session session;

    @BeforeEach
    public void setup() {
        //Start kafka
        kafkaKraftCluster =
                KafkaKraftCluster.newCluster()
                        .withNumBrokers(3)
                        .withContainerEnv("KAFKA_MIN_INSYNC_REPLICAS", "2")
                        // For some tests, we start a consumer after a node has already "exploded"
                        // i.e) we make sure the offset topic can be created by consumers by using replication
                        // factor=2
                        .withContainerEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "2")
                        // a default replication factor of 3 to ensure that the topic is replicated to all
                        // brokers so
                        // when a broker is shut down, it will still be available and can locate a new leader
                        .withContainerEnv("KAFKA_DEFAULT_REPLICATION_FACTOR", "3");

        kafkaKraftCluster.start();

        bridgeKafkaProps = new Properties();
        bridgeKafkaProps.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getContainerHelper().bootstrapServers());

        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(baseConfig)
                        .id("test")
                        .routing(new RoutingConfig.Builder()
                                .addTopics(new RoutingConfig.RoutedTopic.Builder()
                                        .addressTemplate("test.${topic}")
                                        .match("test.customer-topic.*")
                                        .messageType("text"))
                                .build())));

        try {
            amq = amqf.start();
            session = amq.getConnection().createSession(true, Session.SESSION_TRANSACTED);
        } catch (Exception e) {
           fail();
        }
    }

    @AfterEach
    public void tearDown() throws JMSException {
        // noOp
        kafkaKraftCluster.stop();
        amq.close();
        session.close();
    }


    @Test
    void test_JmsSendsToActiveKafkaProducersRecover_When_KafkaCluster_RestartsBrokers() {
        String customerAddress = "test." + "customer-topic1";
        String customerAddress2 = "test." + "customer-topic2";
        String customerAddress3 = "test." + "customer-topic3";

        try {
            Topic topic = session.createTopic(customerAddress);
            Topic topic2 = session.createTopic(customerAddress2);
            Topic topic3 = session.createTopic(customerAddress3);

            try (MessageProducer producer1 = session.createProducer(topic);
                 MessageProducer producer2 = session.createProducer(topic2);
                 MessageProducer producer3 = session.createProducer(topic3);
                 MessageConsumer consumer1 = session.createConsumer(topic);
                 MessageConsumer consumer2 = session.createConsumer(topic2);
                 MessageConsumer consumer3 = session.createConsumer(topic3)) {

                TextMessage message = session.createTextMessage("Message 1");
                message.setJMSReplyTo(topic);
                message.setStringProperty("foo", "bar");
                message.setJMSCorrelationID("FooCorrelationId");
                producer1.send(message);
                session.commit();

                TextMessage message2 = session.createTextMessage("Message 2");
                message2.setJMSReplyTo(topic);
                message2.setStringProperty("foo", "bar");
                message2.setJMSCorrelationID("FooCorrelationId2");
                producer2.send(message2);
                session.commit();

                TextMessage message3 = session.createTextMessage("Message 3");
                message3.setJMSReplyTo(topic);
                message3.setStringProperty("foo", "bar");
                message3.setJMSCorrelationID("FooCorrelationId3");
                producer3.send(message3);
                session.commit();

                // intentionally restarting brokers
                handleContainer2RestartAndContainer3Restart(kafkaKraftCluster);
                // produce sync
                assertDoesNotThrow(
                        () -> {
                            producer1.send(message);
                            producer2.send(message2);
                            producer3.send(message3);
                        });

                List<String> testMessages = new ArrayList<>();
                while (true) {
                    Message consumedMessage = consumer1.receive(5000);
                    if (consumedMessage != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) consumedMessage;
                        if (textMessage.getText() != null) {
                            testMessages.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }

                    Message consumedMessage2 = consumer2.receive(5000);
                    if (consumedMessage2 != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) consumedMessage2;
                        if (textMessage.getText() != null) {
                            testMessages.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }

                    Message consumedMessage3 = consumer3.receive(5000);
                    if (consumedMessage3 != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) consumedMessage3;
                        if (textMessage.getText() != null) {
                            testMessages.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }
                }
                assertEquals(3, testMessages.size());
                consumer1.close();
                consumer2.close();
                consumer3.close();
                producer1.close();
                producer2.close();
                producer3.close();

            } catch (Exception e) {
                log.error("Broken Test", e);
                // assert fails
                fail();
            }
        } catch (Exception e) {
            log.error("Broken Test", e);
            fail();
        }
        kafkaKraftCluster.stop();
    }

    @Test
    void test_JmsSendsToActiveKafkaProducersAndActiveKafkaClientsRecover_When_KafkaCluster_ReplacesBrokers() {

        String customerAddress = "test." + "customer-topic1";

        try {
            Topic topic = session.createTopic(customerAddress);

            int numRecords = 10000;

            try (MessageProducer producer = session.createProducer(topic);
                 MessageConsumer consumer = session.createConsumer(topic)) {

                for (int i = 0; i < numRecords; i++) {
                    TextMessage message = session.createTextMessage("Message 1 " + i);
                    message.setJMSReplyTo(topic);
                    message.setStringProperty("foo", "bar");
                    message.setJMSCorrelationID("FooCorrelationId");
                    producer.send(message);
                    session.commit();
                }

                handleContainer2Restart(kafkaKraftCluster);

                List<String> testMessages = new ArrayList<>();
                while (true) {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) message;
                        if (textMessage.getText() != null) {
                            testMessages.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }
                }
                assertEquals(numRecords, testMessages.size());
                IntStream.range(0, numRecords).forEach(i -> assertTrue(testMessages.get(i).contains("Message 1 " + i)));

                handleContainer3Restart(kafkaKraftCluster);

                for (int i = numRecords; i < numRecords * 2; i++) {
                    TextMessage message = session.createTextMessage("Message 1 " + i);
                    message.setJMSReplyTo(topic);
                    message.setStringProperty("foo", "bar");
                    message.setJMSCorrelationID("FooCorrelationId");
                    producer.send(message);
                    session.commit();
                }


                List<String> testMessages2 = new ArrayList<>();
                while (true) {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) message;
                        if (textMessage.getText() != null) {
                            testMessages2.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }
                }

                assertEquals(numRecords, testMessages2.size());
                IntStream.range(numRecords, numRecords * 2).forEach(i -> assertTrue(testMessages2.contains("Message 1 " + i)));

                consumer.close();
                producer.close();

            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        } catch (IllegalStateException | JMSException e) {
            log.error("BOOM", e);

            } catch (Exception e) {
            log.error("BOOM", e);
            fail();
        }

        kafkaKraftCluster.stop();
    }

    @Test
    void test_JmsSendsToActiveKafkaProducersAndActiveKafkaClientsRecoverWhenBroker_Unexpectedly_Dies() {
        String customerAddress = "test." + "customer-topic1";

        try {
            Topic topic = session.createTopic(customerAddress);

            int numRecords = 10000;

            try (MessageProducer producer = session.createProducer(topic);
                 MessageConsumer consumer = session.createConsumer(topic)) {

                for (int i = 0; i < numRecords; i++) {
                    TextMessage message = session.createTextMessage("Message 1 " + i);
                    message.setJMSReplyTo(topic);
                    message.setStringProperty("foo", "bar");
                    message.setJMSCorrelationID("FooCorrelationId");
                    producer.send(message);
                    session.commit();
                }

                handleContainer2Shutdown(kafkaKraftCluster);

                List<String> testMessages = new ArrayList<>();
                while (true) {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) message;
                        if (textMessage.getText() != null) {
                            testMessages.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }
                }

                assertEquals(numRecords, testMessages.size());
                IntStream.range(0, numRecords).forEach(i -> assertTrue(testMessages.get(i).contains("Message 1 " + i)));

                for (int i = numRecords; i < numRecords * 2; i++) {
                    TextMessage message = session.createTextMessage("Message 1 " + i);
                    message.setJMSReplyTo(topic);
                    message.setStringProperty("foo", "bar");
                    message.setJMSCorrelationID("FooCorrelationId");
                    producer.send(message);
                    session.commit();
                }

                List<String> testMessages2 = new ArrayList<>();
                while (true) {
                    Message message = consumer.receive(5000);
                    if (message != null) {
                        // Process the message
                        TextMessage textMessage = (TextMessage) message;
                        if (textMessage.getText() != null) {
                            testMessages2.add(textMessage.getText());
                        }
                    } else {
                        // No message received within the timeout period
                        break;
                    }
                }

                assertEquals(numRecords, testMessages2.size());
                IntStream.range(numRecords, numRecords * 2).forEach(i -> assertTrue(testMessages2.contains("Message 1 " + i)));
                consumer.close();
                producer.close();

            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            log.error("BOOM", e);
            fail();
        }
        kafkaKraftCluster.stop();
    }

    private void handleContainer2Shutdown(KafkaKraftCluster kafkaKraftCluster) {
        var container1 = kafkaKraftCluster.getContainer(0);
        var container2 = kafkaKraftCluster.getContainer(1);

        container2.stop();
        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () -> {
                            String apiVersions = getBrokerApiVersionsFromActiveBrokers(container1);
                            assertTrue(apiVersions.contains("(id: 0"));
                            assertFalse(apiVersions.contains("(id: 1"));
                            assertTrue(apiVersions.contains("(id: 2"));
                        });

    }


    private void handleContainer2RestartAndContainer3Restart(KafkaKraftCluster kafkaKraftCluster) {
        handleContainer2Restart(kafkaKraftCluster);
        handleContainer3Restart(kafkaKraftCluster);
    }

    /**
     * Note: KRaft requires a majority of nodes to be running. For example, a three-node controller
     * cluster can survive one failure. A five-node controller cluster can survive two failures, and
     * so on.
     */
    private void handleContainer2Restart(KafkaKraftCluster kafkaKraftCluster) {
        var container1 = kafkaKraftCluster.getContainer(0);
        var container2 = kafkaKraftCluster.getContainer(1);
        var oldMappedPortContainer2 = container2.getMappedPort(9093);
        container2.stop();

        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () -> {
                            String apiVersions = getBrokerApiVersionsFromActiveBrokers(container1);
                            assertTrue(apiVersions.contains("(id: 0"));
                            assertTrue(apiVersions.contains("(id: 2"));
                            assertFalse(apiVersions.contains("(id: 1"));
                        });

        // remove duplicate CONTROLLER://0.0.0.0:9094 from listeners before starting
        // this is a workaround for testcontainers automatically adding the controller listener
        // everytime you start a container
        container2.addEnv(
                "KAFKA_LISTENERS",
                container1.getEnvMap().get("KAFKA_LISTENERS").replace(",CONTROLLER://0.0.0.0:9094", ""));
        container2.start();
        assertNotEquals(container1.getMappedPort(9093), oldMappedPortContainer2);

        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () -> {
                            String apiVersions = getBrokerApiVersionsFromActiveBrokers(container1);
                            assertTrue(apiVersions.contains("(id: 0"));
                            assertTrue(apiVersions.contains("(id: 2"));
                            assertTrue(apiVersions.contains("(id: 1"));

                        });
    }

    private void handleContainer3Restart(KafkaKraftCluster kafkaKraftCluster) {
        var container1 = kafkaKraftCluster.getContainer(0);
        var container3 = kafkaKraftCluster.getContainer(2);
        var oldMappedPortContainer3 = container3.getMappedPort(9093);
        container3.stop();

        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () -> {
                            String apiVersions = getBrokerApiVersionsFromActiveBrokers(container1);
                            assertTrue(apiVersions.contains("(id: 0"));
                            assertTrue(apiVersions.contains("(id: 1"));
                            //we are restarting container 3 which has an id of 2 and therefore should not be available as
                            //an active broker in the list
                            assertFalse(apiVersions.contains("(id: 2"));
                        });

        // remove duplicate CONTROLLER://0.0.0.0:9094 from listeners before starting
        // this is a workaround for testcontainers automatically adding the controller listener
        // everytime you start a container
        container3.addEnv(
                "KAFKA_LISTENERS",
                container1.getEnvMap().get("KAFKA_LISTENERS").replace(",CONTROLLER://0.0.0.0:9094", ""));
        container3.start();
        assertNotEquals(container1.getMappedPort(9093), oldMappedPortContainer3);

        //await until all three containers are available
        await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(
                        () -> {
                            String apiVersions = getBrokerApiVersionsFromActiveBrokers(container1);
                            assertTrue(apiVersions.contains("(id: 0"));
                            assertTrue(apiVersions.contains("(id: 2"));
                            assertTrue(apiVersions.contains("(id: 1"));

                        });
    }

    /**
     * @param container the KafkaContainer to execute commands against
     * @return the output of the command `kafka-broker-api-versions --bootstrap-server localhost:9092`
     * @throws IOException
     * @throws InterruptedException
     */
    private String getBrokerApiVersionsFromActiveBrokers(KafkaContainer container)
            throws IOException, InterruptedException {
        log.trace("Executing getBrokerApiVersionsFromActiveBrokers on container: {} ", container);
        var result =
                container.execInContainer(
                        "/bin/kafka-broker-api-versions", "--bootstrap-server", "localhost:9092");
        log.debug("getBrokerApiVersionsFromActiveBrokers exit code: {}", result.getExitCode());
        log.debug("getBrokerApiVersionsFromActiveBrokers stdout: {}", result.getStdout());
        log.debug("getBrokerApiVersionsFromActiveBrokers stderr: {}", result.getStderr());
        return result.getStdout();
    }
}
