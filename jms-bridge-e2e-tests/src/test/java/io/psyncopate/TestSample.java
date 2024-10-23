package io.psyncopate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.RoutingType;
import io.psyncopate.util.constants.ServerType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSample {
    private static final Logger logger = LogManager.getLogger(TestSample.class);
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
    @Disabled
    @DisplayName("Sample test.")
    void sampleTest() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        int messageToBeSent = 2;
        int messageSent = GlobalSetup.getServerSetup().startJmsProducer(queueName, RoutingType.ANYCAST, messageToBeSent);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of sent and received messages should match.");

        int messagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName,  RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().stopMasterServer(), "Master Server should stop.");

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover after producing, switch to slave, then consume messages.")
    void startServersAndGracefulFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        int messageToBeSent = 20;
        int messageSent = GlobalSetup.getServerSetup().startJmsProducer(queueName, RoutingType.ANYCAST, messageToBeSent);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of sent and received messages should match.");

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE, 20 , 1), "Slave Server should be running.");

        int messagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName,  RoutingType.ANYCAST);
        //Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with forced kill after producer is stopped, switch to slave, then consume messages.")
    void startServersAndForceFailoverWithProducerStop() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        //Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        int messageToBeSent = 50;
        int messageSent = GlobalSetup.getServerSetup().startJmsProducer(queueName, RoutingType.ANYCAST, messageToBeSent);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");
        int messagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with forced kill, switch to slave, then consume messages.")
    void startServersAndFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20, 1), "Slave Server should be running.");
        int messagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName,  RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");
        int messageSent=asyncProducerMaster.get();
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Start live server, backup server takes over after live server is killed, with partial and full message consumption.")
    void partialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();
        CompletableFuture<Integer> asyncProducerMaster = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 300L);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE, 20,1), "Slave Server should be running.");
        int messagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        int messageSent=asyncProducerMaster.get();
        int totalMessagesReceived= asyncConsumerMaster.get() + messagesReceived;
        Assertions.assertEquals(messageSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with two producers, switch to slave, then consume all messages..")
    void parallelProducersWithFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        CompletableFuture<Integer> asyncProducerMaster2 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");

        // Wait for the asynchronous operation to complete
        int totalMessagesSent = asyncProducerMaster1.get()+asyncProducerMaster2.get();  // This blocks until the future completes

        int totalMessagesReceived = GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        // Assert that the number of sent and received messages match
        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }
    @Test
    @DisplayName("Master failover with two producers and phased message consumption.")
    void parallelProducersPartialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        CompletableFuture<Integer> asyncProducerMaster2 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 100L);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE, 20,1), "Slave Server should be running.");
        int consumerSlave = GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        int messageSent=asyncProducerMaster1.get() + asyncProducerMaster2.get();
        int messagesReceived= asyncConsumerMaster.get() + consumerSlave;

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with two consumers for partial and complete message consumption.")
    void parallelPartialConsumeAndFailover() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");

        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        CompletableFuture<Integer> asyncConsumerMaster1 = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 100L);
        CompletableFuture<Integer> asyncConsumerMaster2 = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 100L);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE, 20,1), "Slave Server should be running.");
        int consumerSlave = GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Slave Server should stop.");

        int messageSent=asyncProducerMaster1.get();
        int messagesReceived= asyncConsumerMaster1.get() + asyncConsumerMaster2.get() + consumerSlave ;

        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover with producer restart and full message consumption.")
    void failoverWithResumedProductionAndConsume() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");

        CompletableFuture<Integer> slaveProducer = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        CompletableFuture<Integer> messagesReceivedSlave =  GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 0L);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(10), "Slave Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get()+slaveProducer.get();
        int totalMessagesReceived = messagesReceivedSlave.get();

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and complete message consumption.")
    void dualFailoverWithProducerAndConsume() throws Exception {
        Util.isDownloadLog=true;
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killSlaveServer(10), "Slave Server should stop.");

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");

        int totalMessagesReceived =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get()+asyncProducerSlave1.get();
        //int totalMessagesReceived = messagesReceived;

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and phased message consumption.")
    void dualFailoverWithProducerAndStagedConsumption() throws Exception{

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");
        int messagesReceivedfromSlave =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);
        Assertions.assertEquals(asyncProducerMaster1.get(), messagesReceivedfromSlave, "Number of sent and received messages should match.");

        CompletableFuture<Integer> asyncProducerSlave1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killSlaveServer(10), "Slave Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        int messagesReceivedfromMaster =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopMasterServer(), "Slave Server should stop.");

        Assertions.assertEquals(asyncProducerSlave1.get(), messagesReceivedfromMaster, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer, followed by consumer message consumption.")
    void dualKillAndMasterRestartWithMessageConsumption() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");


        Assertions.assertTrue(GlobalSetup.getServerSetup().killSlaveServer(), "Slave Server should stop.");

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        int totalMessagesReceived =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get();

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover, partial consumption, master restart, and complete message consumption.")
    void partialConsumptionWithFailoverAndMasterRestart() throws Exception {
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncConsumerSlave = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 300L);

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");

        int messagesReceived =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get();
        int totalMessagesReceived = messagesReceived + asyncConsumerSlave.get();

        Assertions.assertEquals(totalMessagesSent, totalMessagesReceived, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master failover, producer start, partial and full message consumption with master restart.")
    void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption() throws Exception{
        //Failed testcase
        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,20,1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        CompletableFuture<Integer> asyncConsumerSlave = GlobalSetup.getServerSetup().startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 400L);

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(10), "Master Server should start.");

        int messagesReceivedfromMaster =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);
        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(), "Master Server should stop.");

        int totalReceivedMessageCount = asyncConsumerSlave.get() + messagesReceivedfromMaster;
        Assertions.assertEquals(asyncProducerSlave1.get(), totalReceivedMessageCount, "Number of sent and received messages should match.");
    }

    @Test
    @DisplayName("Master and slave failovers with producer restarts and full message consumption.")
    void tripleFailoverWithProducerRestartsAndFinalConsumption() throws Exception{

        Util.isDownloadLog=true;

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().startSlaveServer(), "Slave Server should start.");
        String queueName = Util.getCurrentMethodNameAsAnycastAddress();

        CompletableFuture<Integer> asyncProducerMaster1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,30,1), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().startMasterServer(15), "Master Server should start.");

        CompletableFuture<Integer> asyncProducerMaster2 = GlobalSetup.getServerSetup().startJmsProducerAsync(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(GlobalSetup.getServerSetup().isServerUp(ServerType.SLAVE,10,1), "Slave Server should be running.");

        int messagesReceivedfromSlave =  GlobalSetup.getServerSetup().startJmsConsumer(queueName, RoutingType.ANYCAST);

        Assertions.assertTrue(GlobalSetup.getServerSetup().stopSlaveServer(), "Master Server should stop.");

        int totalSentMessageCount = asyncProducerMaster1.get()+asyncProducerSlave1.get()+asyncProducerMaster2.get();

        Assertions.assertEquals(totalSentMessageCount, messagesReceivedfromSlave, "Number of sent and received messages should match.");

    }
}
