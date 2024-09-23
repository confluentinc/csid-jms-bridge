package io.confluent.jms.bridge;

import io.confluent.jms.bridge.server.ServerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.JBTestWatcher;
import io.confluent.jms.bridge.util.Util;
import io.confluent.jms.bridge.util.constants.AddressScheme;
import io.confluent.jms.bridge.util.constants.ServerType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JMSQueueTest {
    private static final Logger logger = LogManager.getLogger(JMSQueueTest.class);
    private static final String SHEET_NAME = "JMS Queue Test";

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);

    @BeforeAll
    public static void setup() throws IOException {
        // Create a unique directory for logs
    }

    // Helper method to assert the asynchronous result of message sending
    private void assertAsyncResult(int messageSentToBe, int messageSent) {
        Assertions.assertEquals(messageSentToBe, messageSent, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover after producing, switch to slave, then consume messages.")
    void startServersAndGracefulFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        int messageToBeSent = 20;
        int messageSent = ServerConfig.ServerSetup.startJmsProducer(queueName, messageToBeSent, AddressScheme.ANYCAST);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of sent and received messages should match.");

        Assertions.assertTrue(ServerConfig.ServerSetup.stopMasterServer(), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        int messagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with forced kill after producer is stopped, switch to slave, then consume messages.")
    void startServersAndForceFailoverWithProducerStop() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        int messageToBeSent = 50;
        int messageSent = ServerConfig.ServerSetup.startJmsProducer(queueName, messageToBeSent, AddressScheme.ANYCAST);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int messagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with forced kill, switch to slave, then consume messages.")
    void startServersAndFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int messagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");
        int messageSent = asyncProducerMaster.get();
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Start live server, backup server takes over after live server is killed, with partial and full message consumption.")
    void partialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();
        CompletableFuture<Integer> asyncProducerMaster = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 300L);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int messagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int messageSent = asyncProducerMaster.get();
        int totalMessagesReceived = asyncConsumerMaster.get() + messagesReceived;
        Assertions.assertEquals(messageSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with two producers, switch to slave, then consume all messages..")
    void parallelProducersWithFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        CompletableFuture<Integer> asyncProducerMaster2 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        // Wait for the asynchronous operation to complete
        int totalMessagesSent = asyncProducerMaster1.get() + asyncProducerMaster2.get();  // This blocks until the future completes

        int totalMessagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with two producers and phased message consumption.")
    void parallelProducersPartialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        CompletableFuture<Integer> asyncProducerMaster2 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 100L);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int consumerSlave = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int messageSent = asyncProducerMaster1.get() + asyncProducerMaster2.get();
        int messagesReceived = asyncConsumerMaster.get() + consumerSlave;

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with two consumers for partial and complete message consumption.")
    void parallelPartialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster1 = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 100L);
        CompletableFuture<Integer> asyncConsumerMaster2 = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 100L);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int consumerSlave = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int messageSent = asyncProducerMaster1.get();
        int messagesReceived = asyncConsumerMaster1.get() + asyncConsumerMaster2.get() + consumerSlave;

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with producer restart and full message consumption.")
    void failoverWithResumedProductionAndConsume() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        int totalMessagesSent = asyncProducerMaster1.get() + asyncProducerSlave1.get();
        int totalMessagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and complete message consumption.")
    void dualFailoverWithProducerAndConsume() throws Exception {

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killSlaveServer(10), "Slave Server should stop successfully.");

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");

        int totalMessagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int totalMessagesSent = asyncProducerMaster1.get() + asyncProducerSlave1.get();
        //int totalMessagesReceived = messagesReceived;

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and phased message consumption.")
    void dualFailoverWithProducerAndStagedConsumption() throws Exception {

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");
        int messagesReceivedfromSlave = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertEquals(asyncProducerMaster1.get(), messagesReceivedfromSlave, "Number of sent and received messages should match.");

        CompletableFuture<Integer> asyncProducerSlave1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        Assertions.assertTrue(ServerConfig.ServerSetup.killSlaveServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        int messagesReceivedfromMaster = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopMasterServer(), "Slave Server should stop successfully.");

        Assertions.assertEquals(asyncProducerSlave1.get(), messagesReceivedfromMaster, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer, followed by consumer message consumption.")
    void dualKillAndMasterRestartWithMessageConsumption() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSLav = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        //Assertions.assertTrue(ServerSetup.killSlaveServer(), "Master Server should stop successfully.");
        //Assertions.assertTrue(ServerSetup.startMasterServer(), "Master Server should start successfully.");
        int totalMessagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        //Assertions.assertTrue(ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int totalMessagesSent = asyncProducerMaster1.get();


        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover, partial consumption, master restart, and complete message consumption.")
    void partialConsumptionWithFailoverAndMasterRestart() throws Exception {
        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncConsumerSlave = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 300L);

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");

        int messagesReceived = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int totalMessagesSent = asyncProducerMaster1.get();
        int totalMessagesReceived = messagesReceived + asyncConsumerSlave.get();

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover, producer start, partial and full message consumption with master restart.")
    void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption() throws Exception {

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        CompletableFuture<Integer> asyncConsumerSlave = ServerConfig.ServerSetup.startJmsConsumerAsync(queueName, 300L);

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");

        int messagesReceivedfromMaster = ServerConfig.ServerSetup.startJmsConsumer(queueName);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(), "Master Server should stop successfully.");

        int totalReceivedMessageCount = asyncConsumerSlave.get() + messagesReceivedfromMaster;
        Assertions.assertEquals(asyncProducerSlave1.get(), totalReceivedMessageCount, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and full message consumption.")
    void tripleFailoverWithProducerRestartsAndFinalConsumption() throws Exception {

        Util.isDownloadLog = true;

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");
        String queueName = Util.getMethodNameAsQueueName();

        CompletableFuture<Integer> asyncProducerMaster1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");

        CompletableFuture<Integer> asyncProducerMaster2 = ServerConfig.ServerSetup.startJmsProducerAsync(queueName, AddressScheme.ANYCAST);
        Assertions.assertTrue(ServerConfig.ServerSetup.killMasterServer(10), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.isServerUp(ServerType.SLAVE, 20, 1), "Slave Server should be running.");

        int messagesReceivedFromSlave = ServerConfig.ServerSetup.startJmsConsumer(queueName);

        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        int totalSentMessageCount = asyncProducerMaster1.get() + asyncProducerSlave1.get() + asyncProducerMaster2.get();

        Assertions.assertEquals(totalSentMessageCount, messagesReceivedFromSlave, "Number of sent and received messages should match.");

    }
}
