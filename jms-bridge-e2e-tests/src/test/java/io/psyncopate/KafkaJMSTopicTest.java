package io.psyncopate;

import io.psyncopate.server.ServerSetup;
import io.psyncopate.service.BrokerService;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.MessagingScheme;
import io.psyncopate.util.constants.ServerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@ExtendWith(GlobalSetup.class)
public class KafkaJMSTopicTest {
    private static final Logger logger = LogManager.getLogger(JMSQueueTest.class);
    private static final String SHEET_NAME = "JMS Queue Test";

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);

    private BrokerService brokerService;
    private ServerSetup serverSetup;

    @BeforeAll
    public static void setup() throws IOException {
        //Need to update and upload jms-bridge config files with match rule for Kafka topic routing both for Master and Slave
        GlobalSetup.getServerSetup().updateConfigFile(true);
        GlobalSetup.getServerSetup().updateConfigFile(false);
        //Need to update and upload broker.xml with definitions for Topics (Multicast) for them to be persistent without consumers / subscriptions for routing to Kafka to work from startup - for Master node only.
        GlobalSetup.getServerSetup().updateBrokerXMLFile(true);
    }

    @BeforeEach
    void init() {
        serverSetup = GlobalSetup.getServerSetup();
        brokerService = new BrokerService(serverSetup);
    }

    @Test
    @DisplayName("Kafka Master failover after producing, switch to slave, then consume messages.")
    void startServersAndGracefulFailoverForKafkaTopic() throws Exception {
        String address = Util.getCurrentMethodNameAsKafkaTopic();
        String jmsAddress = address;
        String kafkaTopic = address;
        brokerService.ensureKafkaTopicExists(kafkaTopic);

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        int messageToBeSent = 50;
        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
        int jmsMessageSent = brokerService.startProducer(ServerType.MASTER, MessagingScheme.JMS_MULTICAST, jmsAddress, messageToBeSent);

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);
        int jmsMessagesReceived = brokerService.startConsumer(ServerType.SLAVE, MessagingScheme.JMS_MULTICAST, jmsAddress, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        int totalMessageSent = jmsMessageSent + kafkaMessageSent;

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(jmsMessagesReceived, kafkaMessagesReceived, "Number of jms Received and kafka received messages should match.");
        Assertions.assertEquals(totalMessageSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
        Assertions.assertEquals(totalMessageSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
    }

    @Test
    @DisplayName("Produce to Kafka, failover, consume on JMS.")
    void messagesConsumedOnJmsProducedOnKafkaWithFailover() throws Exception {
        String address = Util.getCurrentMethodNameAsKafkaTopic();
        String jmsAddress = address;
        String kafkaTopic = address;
        brokerService.ensureKafkaTopicExists(kafkaTopic);

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        AtomicBoolean producerStopFlag = new AtomicBoolean(false);
        CompletableFuture<Integer> kafkaMessageSentFuture = brokerService.startAsyncKafkaProducer(kafkaTopic, producerStopFlag);
        new Thread(() -> {
            Util.sleepQuietly(8000);
            producerStopFlag.set(true);
        }).start();

        Assertions.assertTrue(serverSetup.killMasterServer(3), "Master Server killed.");
        producerStopFlag.set(true);
        int kafkaMessagesSent = kafkaMessageSentFuture.get();
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);
        int jmsMessagesReceived = brokerService.startConsumer(ServerType.SLAVE, MessagingScheme.JMS_MULTICAST, jmsAddress, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(jmsMessagesReceived, kafkaMessagesReceived, "Number of jms Received and kafka received messages should match.");
        Assertions.assertEquals(kafkaMessagesSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
        Assertions.assertEquals(kafkaMessagesSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
    }

    @Test
    @DisplayName("Kafka Queue Start live server, backup server takes over after live server is killed, with partial and full message consumption.")
    void partialConsumeAndFailoverForKafkaTopic() throws Exception {
        String address = Util.getCurrentMethodNameAsKafkaTopic();
        String jmsAddress = address;
        String kafkaTopic = address;
        brokerService.ensureKafkaTopicExists(kafkaTopic);

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        int messageToBeSent = 50;
        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
        CompletableFuture<Integer> asyncJMSProducerMaster = brokerService.startAsyncJmsProducer(ServerType.MASTER, MessagingScheme.JMS_MULTICAST, jmsAddress);
        CompletableFuture<Integer> asyncJMSConsumerMaster = brokerService.startAsyncConsumer(ServerType.MASTER, MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
        Assertions.assertTrue(serverSetup.killMasterServer(3), "Master Server should be force killed.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int jmsMessagesReceivedSlave = brokerService.startConsumer(ServerType.SLAVE, MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);


        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        int totalMessageSent = asyncJMSProducerMaster.get() + kafkaMessageSent;
        int totalJmsMessageReceived = asyncJMSConsumerMaster.get() + jmsMessagesReceivedSlave;
        int totalKafkaMessageReceived = kafkaMessagesReceived;


        // Assert that the number of sent and received messages match
        Assertions.assertEquals(totalJmsMessageReceived, totalKafkaMessageReceived, "Number of jms Received and kafka received messages should match.");
        Assertions.assertEquals(totalMessageSent, totalJmsMessageReceived, "Number of sent and JMS received messages should match.");
        Assertions.assertEquals(totalMessageSent, totalKafkaMessageReceived, "Number of sent and Kafka received messages should match.");
    }

    @Test
    @DisplayName("Kafka Master and slave failovers with producer, followed by consumer message consumption.")
    void dualKillAndMasterRestartWithMessageConsumptionForKafkaTopic() throws Exception {
        String address = Util.getCurrentMethodNameAsKafkaTopic();
        String jmsAddress = address;
        String kafkaTopic = address;
        brokerService.ensureKafkaTopicExists(kafkaTopic);

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        int messageToBeSent = 50;
        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncJmsProducer(ServerType.MASTER, MessagingScheme.JMS_MULTICAST, jmsAddress);
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should kill.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        Assertions.assertTrue(serverSetup.killSlaveServer(), "Slave Server should Kill.");
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should Restart.");
        int jmsMessagesReceived = brokerService.startConsumer(ServerType.MASTER, MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);


        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");

        int totalMessageSent = asyncProducerMaster.get() + kafkaMessageSent;

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(jmsMessagesReceived, kafkaMessagesReceived, "Number of jms Received and kafka received messages should match.");
        Assertions.assertEquals(totalMessageSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
        Assertions.assertEquals(totalMessageSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
    }
//    @Test
//    @DisplayName("Kafka Master failover with forced kill after producer is stopped, switch to slave, then consume messages.")
//    void startServersAndForceFailoverWithProducerStopForKafkaTopic() throws Exception {
//        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
//        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
//
//        String kafkaTopic = Util.getCurrentMethodNameAsKafkaTopic();
//        String jmsAddress = this.appendPrefix(kafkaTopic);
//
//
//        int messageToBeSent = 50;
//        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
//        int jmsMessageSent = brokerService.startProducer(MessagingScheme.JMS_MULTICAST, jmsAddress, messageToBeSent);
//
//        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");
//        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
//        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);
//        int jmsMessagesReceived = brokerService.startConsumer(MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
//        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");
//
//        int totalMessageSent= jmsMessageSent + kafkaMessageSent;
//
//        // Assert that the number of sent and received messages match
//        Assertions.assertEquals(jmsMessagesReceived, kafkaMessagesReceived, "Number of jms Received and kafka received messages should match.");
//        Assertions.assertEquals(totalMessageSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
//        Assertions.assertEquals(totalMessageSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
//
//    }
//
//
//    @Test
//    @DisplayName("Kafka Master failover with forced kill, switch to slave, then consume messages.")
//    void startServersAndFailoverForKafkaTopic() throws Exception {
//        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
//        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
//
//        String kafkaTopic = Util.getCurrentMethodNameAsKafkaTopic();
//        String jmsAddress = this.appendPrefix(kafkaTopic);
//
//        int messageToBeSent = 50;
//        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
//        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncProducer(MessagingScheme.JMS_MULTICAST, jmsAddress);
//        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should kill.");
//        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE,10,10), "Slave Server should be running.");
//        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);
//        int jmsMessagesReceived = brokerService.startConsumer(MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
//
//
//        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");
//
//        int totalMessageSent= asyncProducerMaster.get() + kafkaMessageSent;
//
//        // Assert that the number of sent and received messages match
//        Assertions.assertEquals(jmsMessagesReceived, kafkaMessagesReceived, "Number of jms Received and kafka received messages should match.");
//        Assertions.assertEquals(totalMessageSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
//        Assertions.assertEquals(totalMessageSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
//
//    }
//
//
//    @Test
//    @DisplayName("Kafka Master failover with producer restart and full message consumption.")
//    void FailoverWithResumedProductionAndConsumeForKafkaTopic() throws Exception {
//        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
//        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
//
//        String kafkaTopic = Util.getCurrentMethodNameAsKafkaTopic();
//        String jmsAddress = this.appendPrefix(kafkaTopic);
//
//        int messageToBeSent = 50;
//        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should kill.");
//        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE,10,10), "Slave Server should be running.");
//        int kafkaMessageSent = brokerService.startProducer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic, messageToBeSent);
//        int jmsMessageSent = brokerService.startProducer(MessagingScheme.JMS_MULTICAST, jmsAddress, messageToBeSent);
//
//        int kafkaMessagesReceived = brokerService.startConsumer(ServerType.KAFKA, MessagingScheme.KAFKA_TOPIC, kafkaTopic);
//        int jmsMessagesReceived = brokerService.startConsumer(MessagingScheme.JMS_MULTICAST, jmsAddress, kafkaTopic);
//
//        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");
//
//        int totalMessageSent= jmsMessageSent + kafkaMessageSent;
//
//        // Assert that the number of sent and received messages match
//        Assertions.assertEquals(totalMessageSent, jmsMessagesReceived, "Number of sent and JMS received messages should match.");
//        Assertions.assertEquals(totalMessageSent, kafkaMessagesReceived, "Number of sent and Kafka received messages should match.");
//
//    }


    String appendPrefix(String address) {
        return "kafka." + address;
    }
}
