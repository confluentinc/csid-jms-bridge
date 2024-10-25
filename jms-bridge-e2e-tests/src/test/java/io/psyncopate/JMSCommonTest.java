package io.psyncopate;

import io.psyncopate.server.ServerSetup;
import io.psyncopate.service.BrokerService;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.MessagingScheme;
import io.psyncopate.util.constants.ServerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JMSCommonTest {
    private static final Logger logger = LogManager.getLogger(JMSCommonTest.class);
    private static final String SHEET_NAME = "JMS Topic Test";
    private final BrokerService brokerService;
    private final ServerSetup serverSetup;

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);
    private static Integer topicCount = 0;

    public JMSCommonTest(BrokerService brokerService, ServerSetup serverSetup){
        this.brokerService = brokerService;
        this.serverSetup = serverSetup;
    }
    @BeforeAll
    public static void setup() throws IOException, InterruptedException {
        // Create a unique directory for logs
    }
    
    
    
    // Helper method to assert the asynchronous result of message sending
    private void assertAsyncResult(int messageSentToBe, int messageSent) {
        Assertions.assertEquals(messageSentToBe, messageSent, "Number of sent and received messages should match.");
    }

    private void validateReceivedMessages(int messageSent, int messagesReceived) {
        Assertions.assertNotEquals(0, messagesReceived, "Number of received messages should not be zero");
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

    public void sampleTest(MessagingScheme messagingScheme, int messageToBeSent) throws Exception {

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int messageSent = brokerService.startProducer(ServerType.MASTER, messagingScheme, address, messageToBeSent);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of message to be sent and message sent messages should match.");

        int messagesReceived = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");

        validateReceivedMessages(messageSent, messagesReceived);

    }

    public void startServersAndGracefulFailover(MessagingScheme messagingScheme, int messageToBeSent) throws Exception {

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int messageSent = brokerService.startProducer(ServerType.MASTER, messagingScheme, address, messageToBeSent);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of message to be sent and message sent messages should match.");

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 40, 4), "Slave Server should be running.");

        int messagesReceived = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        validateReceivedMessages(messageSent, messagesReceived);

    }

    public void startServersAndForceFailoverWithProducerStop(MessagingScheme messagingScheme, int messageToBeSent) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");

        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int messageSent = brokerService.startProducer(ServerType.MASTER, messagingScheme, address, messageToBeSent);
        Assertions.assertEquals(messageToBeSent, messageSent, "Number of message to be sent and message sent messages should match.");

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        int messagesReceived = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        validateReceivedMessages(messageSent, messagesReceived);

    }

    public void startServersAndFailover(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int messagesReceived = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");
        int messageSent = asyncProducerMaster.get();
        validateReceivedMessages(messageSent, messagesReceived);
    }


    public void partialConsumeAndFailover(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncConsumerMaster = brokerService.startAsyncConsumer(ServerType.MASTER, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int messagesReceived = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        int messageSent = asyncProducerMaster.get();
        int totalMessagesReceived = asyncConsumerMaster.get() + messagesReceived;
        validateReceivedMessages(messageSent, totalMessagesReceived);
    }

    public void parallelProducersWithFailover(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        // Wait for the asynchronous operation to complete
        int totalMessagesSent = asyncProducerMaster1.get() + asyncProducerMaster2.get();  // This blocks until the future completes

        int totalMessagesReceived = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        // Assert that the number of sent and received messages match
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }

    public void parallelProducersPartialConsumeAndFailover(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncConsumerMaster = brokerService.startAsyncConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int consumerSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Slave Server should stop.");

        int messageSent = asyncProducerMaster1.get() + asyncProducerMaster2.get();
        int messagesReceived = asyncConsumerMaster.get() + consumerSlave;

        validateReceivedMessages(messageSent, messagesReceived);
    }

    public void failoverWithResumedProductionAndConsume(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave = brokerService.startAsyncProducer(ServerType.SLAVE, messagingScheme, address);

        CompletableFuture<Integer> asyncmessagesReceivedSlave = brokerService.startAsyncConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopSlaveServer(10), "Slave Server should stop.");

        int totalMessagesSent = asyncProducerMaster.get() + asyncProducerSlave.get();
        int totalMessagesReceived = asyncmessagesReceivedSlave.get();
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }


    public void dualFailoverWithProducerAndConsume(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncProducer(ServerType.SLAVE, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killSlaveServer(10), "Slave Server should stop.");

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");

        int totalMessagesReceived = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get() + asyncProducerSlave1.get();
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }


    public void dualFailoverWithProducerAndStagedConsumption(MessagingScheme messagingScheme) throws Exception {

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);
        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int messagesReceivedfromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        //Assertions.assertEquals(asyncProducerMaster1.get(), messagesReceivedfromSlave, "Number of sent and received messages should match.");
        validateReceivedMessages(asyncProducerMaster1.get(), messagesReceivedfromSlave);

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncProducer(ServerType.SLAVE, messagingScheme, address);
        Assertions.assertTrue(serverSetup.killSlaveServer(10), "Slave Server should stop.");
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        int messagesReceivedFromMaster = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Slave Server should stop.");
        int totalMessageSent = asyncProducerSlave1.get();
        validateReceivedMessages(totalMessageSent, messagesReceivedFromMaster);
    }


    ///sabarish
    public void dualKillAndMasterRestartWithMessageConsumption(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        Assertions.assertTrue(serverSetup.killSlaveServer(), "Slave Server should stop.");

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        int totalMessagesReceived = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get();
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }

    //new
    public void partialConsumptionWithFailoverAndMasterRestart(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncConsumerSlave = brokerService.startAsyncConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");

        int messagesReceived = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get();
        int totalMessagesReceived = messagesReceived + asyncConsumerSlave.get();
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }

    //new
    public void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption(MessagingScheme messagingScheme) throws Exception {
        //Failed testcase
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncProducer(ServerType.SLAVE, messagingScheme, address);

        CompletableFuture<Integer> asyncConsumerSlave = brokerService.startAsyncConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.startMasterServer(10), "Master Server should start.");

        int messagesReceivedfromMaster = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");

        int totalReceivedMessageCount = asyncConsumerSlave.get() + messagesReceivedfromMaster;
        validateReceivedMessages(asyncProducerSlave1.get(), totalReceivedMessageCount);
    }

    //new
    public void tripleFailoverWithProducerRestartsAndFinalConsumption(MessagingScheme messagingScheme) throws Exception {

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncProducer(ServerType.SLAVE, messagingScheme, address);

        Assertions.assertTrue(serverSetup.startMasterServer(15), "Master Server should start.");

        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncProducer(ServerType.MASTER, messagingScheme, address);


        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        int messagesReceivedfromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);


        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Master Server should stop.");

        int totalSentMessageCount = asyncProducerMaster1.get() + asyncProducerSlave1.get() + asyncProducerMaster2.get();
        validateReceivedMessages(totalSentMessageCount, messagesReceivedfromSlave);

    }
}
