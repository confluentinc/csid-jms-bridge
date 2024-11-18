package io.psyncopate;

import io.psyncopate.client.JMSClient;
import io.psyncopate.server.ServerSetup;
import io.psyncopate.service.BrokerService;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.MessagingScheme;
import io.psyncopate.util.constants.RoutingType;
import io.psyncopate.util.constants.ServerType;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.jms.*;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JMSCommonTest {
    private static final Logger logger = LogManager.getLogger(JMSCommonTest.class);
    private static final String SHEET_NAME = "JMS Topic Test";
    private final BrokerService brokerService;
    private final ServerSetup serverSetup;

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);
    private static Integer topicCount = 0;

    public JMSCommonTest(BrokerService brokerService, ServerSetup serverSetup) {
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
        assertThat(messageSent).as("Number of sent and received messages should match with a difference of up to 3.").isCloseTo(messagesReceived, Offset.offset(3));
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
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

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

        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
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
        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
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

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

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

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
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

        CompletableFuture<Integer> asyncProducerMaster = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave = brokerService.startAsyncJmsProducer(ServerType.SLAVE, messagingScheme, address);

        CompletableFuture<Integer> asyncmessagesReceivedSlave = brokerService.startAsyncConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopSlaveServer(10), "Slave Server should stop.");

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Integer messagesReceivedMaster = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);
        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster.get() + asyncProducerSlave.get();
        int totalMessagesReceived = asyncmessagesReceivedSlave.get() + messagesReceivedMaster;
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }


    public void dualFailoverWithProducerAndConsume(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncJmsProducer(ServerType.SLAVE, messagingScheme, address);

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

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);
        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");
        int messagesReceivedfromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);
        //Assertions.assertEquals(asyncProducerMaster1.get(), messagesReceivedfromSlave, "Number of sent and received messages should match.");
        validateReceivedMessages(asyncProducerMaster1.get(), messagesReceivedfromSlave);

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncJmsProducer(ServerType.SLAVE, messagingScheme, address);
        Assertions.assertTrue(serverSetup.killSlaveServer(10), "Slave Server should stop.");
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        int messagesReceivedFromMaster = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Slave Server should stop.");
        int totalMessageSent = asyncProducerSlave1.get();
        validateReceivedMessages(totalMessageSent, messagesReceivedFromMaster);
    }

    public void dualKillAndMasterRestartWithMessageConsumption(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        Assertions.assertTrue(serverSetup.killSlaveServer(), "Slave Server should stop.");

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        int totalMessagesReceived = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");

        int totalMessagesSent = asyncProducerMaster1.get();
        validateReceivedMessages(totalMessagesSent, totalMessagesReceived);
    }

    public void partialConsumptionWithFailoverAndMasterRestart(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

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

    public void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption(MessagingScheme messagingScheme) throws Exception {
        //Failed testcase
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncJmsProducer(ServerType.SLAVE, messagingScheme, address);

        CompletableFuture<Integer> asyncConsumerSlave = brokerService.startAsyncConsumer(ServerType.SLAVE, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.startMasterServer(10), "Master Server should start.");

        int messagesReceivedfromMaster = brokerService.startConsumer(ServerType.MASTER, messagingScheme, address, address);

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");

        int totalReceivedMessageCount = asyncConsumerSlave.get() + messagesReceivedfromMaster;
        validateReceivedMessages(asyncProducerSlave1.get(), totalReceivedMessageCount);
    }

    public void tripleFailoverWithProducerRestartsAndFinalConsumption(MessagingScheme messagingScheme) throws Exception {

        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        CompletableFuture<Integer> asyncProducerMaster1 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);

        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        CompletableFuture<Integer> asyncProducerSlave1 = brokerService.startAsyncJmsProducer(ServerType.SLAVE, messagingScheme, address);

        Assertions.assertTrue(serverSetup.startMasterServer(15), "Master Server should start.");

        CompletableFuture<Integer> asyncProducerMaster2 = brokerService.startAsyncJmsProducer(ServerType.MASTER, messagingScheme, address);


        Assertions.assertTrue(serverSetup.killMasterServer(10), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        int messagesReceivedfromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, address, address);


        Assertions.assertTrue(serverSetup.stopSlaveServer(), "Master Server should stop.");

        int totalSentMessageCount = asyncProducerMaster1.get() + asyncProducerSlave1.get() + asyncProducerMaster2.get();
        validateReceivedMessages(totalSentMessageCount, messagesReceivedfromSlave);

    }

    /**
     * Transacted produce to master, verify that no messages to consume (as not committed), commit, failover, non-transacted consume from slave. Verify that all produced messages are consumed
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionProduceFailover(MessagingScheme messagingScheme) throws Exception {
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        //Produce and commit messages to Master
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getProducerDestination(messagingScheme, session, address);
            try (MessageProducer producer = session.createProducer(destination)) {
                jmsClient.produceMessages(producer, session, numberOfMessagesToProduce);
                int received = brokerService.startConsumer(ServerType.MASTER, messagingScheme, -1, address, address);
                assertThat(received).as("Shouldn't consume any messages as producer session was not committed yet.").isEqualTo(0);
                session.commit();
            } // producer close
        } // session close

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //Consume from Slave
        int receivedFromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, -1, address, address);

        validateReceivedMessages(numberOfMessagesToProduce, receivedFromSlave);
    }

    /**
     * Transacted produce to master, verify that no messages to consume (as not committed), commit, consume. Verify that all produced messages are consumed
     * <p>
     * Applicable to Queues and Topics
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionProduceOnMaster(MessagingScheme messagingScheme) throws Exception {
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        int receivedFromMaster = 0;
        //Produce and commit messages to Master
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getProducerDestination(messagingScheme, session, address);
            try (MessageProducer producer = session.createProducer(destination)) {
                jmsClient.produceMessages(producer, session, numberOfMessagesToProduce);
                try (Session consumerSession = serverSetup.createSession(ServerType.MASTER, false, Session.AUTO_ACKNOWLEDGE)) {
                    Destination consumerDestination = getConsumerDestinationForScheme(messagingScheme, consumerSession, address);
                    try (MessageConsumer consumer = consumerSession.createConsumer(consumerDestination)) {
                        while (true) {
                            Message msg = consumer.receive(3000);
                            if (msg != null) {
                                receivedFromMaster++;
                            } else {
                                break;
                            }
                        }
                        logger.debug("Consumed {} messages, before Producer session commit", receivedFromMaster);
                        assertThat(receivedFromMaster).as("Shouldn't consume any messages as producer session was not committed yet.").isEqualTo(0);

                        session.commit();
                        while (true) {
                            Message msg = consumer.receive(3000);
                            if (msg != null) {
                                receivedFromMaster++;
                            } else {
                                break;
                            }
                        }
                        logger.debug("Consumed {} messages, after Producer session commit", receivedFromMaster);
                    }// consumer close
                }//consumer session close

            } // producer close
        } // session close
        validateReceivedMessages(numberOfMessagesToProduce, receivedFromMaster);
    }

    private Destination getProducerDestination(MessagingScheme messagingScheme, Session session, String address) throws JMSException {
        Destination destination = RoutingType.ANYCAST.equals(messagingScheme.getRoutingType()) ? session.createQueue(address) : session.createTopic(address);
        return destination;
    }

    /**
     * Produce to master, transacted consume but not commit, close consumer session, consume again, commit, verify no messages to consume as committed acks.
     * <p>
     * Applicable to Queues only due to Anycast / Multicast differences
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionConsumeOnMaster(MessagingScheme messagingScheme) throws Exception {
        if (messagingScheme.getRoutingType().equals(RoutingType.MULTICAST)) {
            verifyLocalTransactionConsumeOnMasterTopic();
        } else if (messagingScheme.getRoutingType().equals(RoutingType.ANYCAST)) {
            verifyLocalTransactionConsumeOnMasterQueue();
        } else {
            throw new IllegalArgumentException("Routing type of ANYCAST or MULTICAST is required");
        }
    }

    private void verifyLocalTransactionConsumeOnMasterQueue() throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_ANYCAST;
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        int numberReceived = 0;
        int numProduced = 0;

        //Produce non-transacted to Master
        numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);

        //Consume with Transaction from Master but dont commit
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            try (MessageConsumer consumer = session.createConsumer(destination)) {


                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
                assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
            } // close consumer without commit
        } // close session used by consumer  without commit

        //Consume with Transaction from Master and commit
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            try (MessageConsumer consumer = session.createConsumer(destination)) {

                numberReceived = 0;
                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            } // close consumer
            assertThat(numberReceived).as("Should consume again all messages produced as previous consumer transaction was not committed").isEqualTo(numProduced);
            session.commit();
        } //close session after commit done

        //Consume from Master again - should not get anything as last consume was committed / acked
        numberReceived = jmsClient.consumeMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), -1, 0L);


        assertThat(numberReceived).as("Shouldn't consume any messages as consumer committed transaction.").isEqualTo(0);
    }

    private void verifyLocalTransactionConsumeOnMasterTopic() throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_MULTICAST;
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        int numberReceived = 0;
        int numProduced = 0;
        //Consume with Transaction from Master but dont commit

        try (Session session2 = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination2 = getConsumerDestinationForScheme(messagingScheme, session2, address);

            try (MessageConsumer consumer2 = session2.createConsumer(destination2)) {
                try (Session session1 = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
                    Destination destination1 = getConsumerDestinationForScheme(messagingScheme, session1, address);
                    try (MessageConsumer consumer1 = session1.createConsumer(destination1)) {
                        //Produce non-transacted to Master
                        numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);


                        while (true) {
                            Message msg = consumer1.receive(500);
                            if (msg != null) {
                                numberReceived++;
                            } else {
                                break;
                            }
                        }
                        assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
                    } // close consumer1 without commit
                } // close session1 used by consumer1 without commit


                //Consume with Transaction from Master and commit
                numberReceived = 0;
                while (true) {
                    Message msg = consumer2.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            } // close consumer2
            assertThat(numberReceived).as("Should consume again all messages produced as previous consumer transaction was not committed").isEqualTo(numProduced);
            session2.commit();
        } //close session2 after commit done
    }

    /**
     * Transacted produce to master, verify that no messages to consume (as not committed), failover, verify that no message to consume on slave - ensures that aborted commit rolled back correctly.
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionProduceOnMasterRollbackOnFailover(MessagingScheme messagingScheme) throws Exception {
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        //Produce to Master with Transaction but dont commit
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getProducerDestination(messagingScheme, session, address);
            try (MessageProducer producer = session.createProducer(destination)) {
                jmsClient.produceMessages(producer, session, numberOfMessagesToProduce);
            } // producer close
        } // session close

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //Consume from slave - shouldn't be anything to consume as transaction was not committed
        int receivedFromSlave = brokerService.startConsumer(ServerType.SLAVE, messagingScheme, -1, address, address);
        validateReceivedMessages(0, receivedFromSlave);
    }

    /**
     * Produce to master, consume in transaction but don't commit, failover, verify that messages are consumed again (as 1st consume rolled back)
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionConsumeOnMasterRollbackOnFailover(MessagingScheme messagingScheme) throws Exception {
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        if (messagingScheme == MessagingScheme.JMS_ANYCAST) {
            verifyLocalTransactionConsumeOnMasterRollbackOnFailoverQueue(address);
        } else {
            verifyLocalTransactionConsumeOnMasterRollbackOnFailoverTopic(address);
        }
    }

    private void verifyLocalTransactionConsumeOnMasterRollbackOnFailoverTopic(String address) throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_MULTICAST;
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String clientId = "test-client";


        //Consume in transaction from Master but dont commit
        try (Session session = jmsClient.createSessionWithDefaultConnection(ServerType.MASTER, true, Session.SESSION_TRANSACTED, clientId)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);

            int numberReceived = 0;
            int numProduced;
            try (TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, "test-subscriber")) {
                //Produce non-transacted to Master - have to do it after the topic subscriber is created ( as topic has to have a destination to persist messages)
                numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);

                while (true) {
                    Message msg = topicSubscriber.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
            assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
        }

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        int numberReceived = 0;
        //should consume again from Slave as 1st consume from master was not committed
        try (Session session = jmsClient.createSessionWithDefaultConnection(ServerType.SLAVE, false, Session.AUTO_ACKNOWLEDGE, clientId)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);

            try (TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, "test-subscriber")) {
                while (true) {
                    Message msg = topicSubscriber.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
        }
        validateReceivedMessages(numberOfMessagesToProduce, numberReceived);
    }

    private void verifyLocalTransactionConsumeOnMasterRollbackOnFailoverQueue(String address) throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_ANYCAST;
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        //Produce non-transacted to Master
        int numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);

        //Consume in transaction from Master but dont commit
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            int numberReceived = 0;
            try (MessageConsumer consumer = session.createConsumer(destination)) {
                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
            assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
        }

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //should consume again from Slave as 1st consume from master was not committed
        int receivedFromSlave = jmsClient.consumeMessages(ServerType.SLAVE, address, messagingScheme.getRoutingType(), -1, 0L);
        validateReceivedMessages(numberOfMessagesToProduce, receivedFromSlave);
    }


    /**
     * Produce to master, consume in transaction, commit, failover, verify that messages are cannot be consumed again (as 1st consume committed / acked)
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyLocalTransactionConsumeOnMasterCommitOnFailover(MessagingScheme messagingScheme) throws Exception {
        String address = Util.getParentMethodNameAsAddress(messagingScheme);
        if (messagingScheme == MessagingScheme.JMS_ANYCAST) {
            verifyLocalTransactionConsumeOnMasterCommitOnFailoverQueue(address);
        } else {
            verifyLocalTransactionConsumeOnMasterCommitOnFailoverTopic(address);
        }
    }

    private void verifyLocalTransactionConsumeOnMasterCommitOnFailoverTopic(String address) throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_MULTICAST;
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        String clientId = "test-client";


        //Consume in transaction from Master but dont commit
        try (Connection connection = jmsClient.getConnection(ServerType.MASTER, clientId)) {
            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
                Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);

                int numberReceived = 0;
                int numProduced;
                try (TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, "test-subscriber")) {
                    //Produce non-transacted to Master - have to do it after the topic subscriber is created ( as topic has to have a destination to persist messages)
                    numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);

                    while (true) {
                        Message msg = topicSubscriber.receive(500);
                        if (msg != null) {
                            numberReceived++;
                        } else {
                            break;
                        }
                    }
                } // close topic subscriber
                assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
                session.commit();
            }// close session
        } // close connection
        //Verify nothing to consume on Master after the previous consumer / subscriber commit - this time non-transacted session
        try (Session session = jmsClient.createSessionWithDefaultConnection(ServerType.MASTER, false, Session.AUTO_ACKNOWLEDGE, clientId)) {
            int numberReceived = 0;
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);

            try (TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, "test-subscriber")) {
                while (true) {
                    Message msg = topicSubscriber.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
            assertThat(numberReceived).as("Shouldn't consume any messages as previous consumer committed transaction.").isEqualTo(0);
        }

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        int numberReceived = 0;
        //should consume again from Slave as 1st consume from master was not committed
        try (Session session = jmsClient.createSessionWithDefaultConnection(ServerType.SLAVE, false, Session.AUTO_ACKNOWLEDGE, clientId)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);

            try (TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, "test-subscriber")) {
                while (true) {
                    Message msg = topicSubscriber.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
        }
        assertThat(numberReceived).as("Shouldn't consume any messages as previous consumer committed transaction on Master already.").isEqualTo(0);
    }

    private void verifyLocalTransactionConsumeOnMasterCommitOnFailoverQueue(String address) throws Exception {
        MessagingScheme messagingScheme = MessagingScheme.JMS_ANYCAST;
        int numberOfMessagesToProduce = 50;
        JMSClient jmsClient = new JMSClient();
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");

        //Produce non-transacted to Master
        int numProduced = jmsClient.produceMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), numberOfMessagesToProduce);

        //Consume in transaction from Master and commit
        try (Session session = serverSetup.createSession(ServerType.MASTER, true, Session.SESSION_TRANSACTED)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            int numberReceived = 0;
            try (MessageConsumer consumer = session.createConsumer(destination)) {
                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceived++;
                    } else {
                        break;
                    }
                }
            }
            assertThat(numberReceived).as("Should consume all messages produced").isEqualTo(numProduced);
            session.commit();
        }

        //Verify that nothing to consume from Master after commit
        int verifyReceiveAfterCommit = jmsClient.consumeMessages(ServerType.MASTER, address, messagingScheme.getRoutingType(), -1, 0L);
        assertThat(verifyReceiveAfterCommit).as("Shouldn't consume any messages as previous consumer committed transaction.").isEqualTo(0);

        //Failover to Slave
        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //should consume nothing from Slave as consumed and committed on Master
        int receivedFromSlave = jmsClient.consumeMessages(ServerType.SLAVE, address, messagingScheme.getRoutingType(), -1, 0L);
        assertThat(receivedFromSlave).as("Shouldn't consume any messages as consumer on Master committed transaction.").isEqualTo(0);
    }

    /**
     * Test highest used message id is not persisted and subsequently bumped by Integer Max abrupt (kill) fail-over and fail-back
     * Produce to Master to get some msgIds, kill Master, failover to Slave, produce to Slave, kill Slave, start Master - produce to Master
     * consume to verify that highest message Id is above Int max * 2, but below Int max * 3 (as two abrupt kills - on Master and on Slave).
     * Msg Id is bumped by Int max if it was not persisted to store and read back correctly.
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyMessageIdIsIncreasedCorrectlyOnKillFailoverAndFailback(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        int numberOfMessagesToSend = 20;
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        //Produce to master and failover
        int numberOfMessagesProduced = brokerService.startProducer(ServerType.MASTER, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertEquals(numberOfMessagesToSend, numberOfMessagesProduced, "Number of message to be sent and message sent messages should match.");

        Assertions.assertTrue(serverSetup.killMasterServer(), "Master Server should stop (killed).");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //Produce to slave and failback
        numberOfMessagesProduced += brokerService.startProducer(ServerType.SLAVE, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertTrue(serverSetup.killSlaveServer(), "Slave Server should stop (killed).");
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.MASTER, 10, 10), "Master Server should be running.");

        //Produce to Master now after the failover and failback - this is to generate messages with fresh message ids
        //if message ids where persisted and read back in fine - then we still should be in low message ids range (below Integer Max).
        numberOfMessagesProduced += brokerService.startProducer(ServerType.MASTER, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertEquals(numberOfMessagesToSend * 3, numberOfMessagesProduced, "Number of messages actually sent should match number to send * 3 - as we produced 3 times.");

        long highestSeenMsgIdOnMaster = 0;
        int numberReceivedFromMaster = 0;
        try (Session session = serverSetup.createSession(ServerType.MASTER, false, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            try (MessageConsumer consumer = session.createConsumer(destination)) {
                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceivedFromMaster++;
                        long msgId = ((ActiveMQTextMessage) msg).getCoreMessage().getMessageID();
                        if (highestSeenMsgIdOnMaster < msgId) {
                            highestSeenMsgIdOnMaster = msgId;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        assertThat(numberReceivedFromMaster).as("Should consume all messages produced").isEqualTo(numberOfMessagesProduced);
        assertThat(highestSeenMsgIdOnMaster).as("Highest seen message id as consumed on Master should be below Integer.MAX but above one seen on Slave.").isBetween(((long) Integer.MAX_VALUE) * 2, ((long) Integer.MAX_VALUE) * 3);
    }

    private Destination getConsumerDestinationForScheme(MessagingScheme messagingScheme, Session session, String address) throws JMSException {
        return RoutingType.ANYCAST.equals(messagingScheme.getRoutingType()) ? session.createQueue(address) : session.createTopic(address + "::" + address);
    }

    /**
     * Test highest used message id is persisted and read back on fail-over and fail-back
     * Produce to Master to get some msgIds, failover to Slave, produce to Slave, fail-back to Master - produce to Master
     * consume to verify that highest message Id is below Int max.
     * Msg Id is bumped by Int max if it was not persisted to store and read back correctly.
     *
     * @param messagingScheme
     * @throws Exception
     */
    public void verifyMessageIdPersistedAndReadBackOnGracefulFailoverAndFailback(MessagingScheme messagingScheme) throws Exception {
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.startSlaveServer(), "Slave Server should start.");
        int numberOfMessagesToSend = 20;
        String address = Util.getParentMethodNameAsAddress(messagingScheme);

        //Produce to master and failover
        int numberOfMessagesProduced = brokerService.startProducer(ServerType.MASTER, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertEquals(numberOfMessagesToSend, numberOfMessagesProduced, "Number of message to be sent and message sent messages should match.");

        Assertions.assertTrue(serverSetup.stopMasterServer(), "Master Server should stop.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.SLAVE, 10, 10), "Slave Server should be running.");

        //Produce to slave and failback
        numberOfMessagesProduced += brokerService.startProducer(ServerType.SLAVE, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertTrue(serverSetup.startMasterServer(), "Master Server should start.");
        Assertions.assertTrue(serverSetup.isServerUp(ServerType.MASTER, 10, 10), "Master Server should be running.");

        //Produce to Master now after the failover and failback - this is to generate messages with fresh message ids
        //if message ids where persisted and read back in fine - then we still should be in low message ids range (below Integer Max).
        numberOfMessagesProduced += brokerService.startProducer(ServerType.MASTER, messagingScheme, address, numberOfMessagesToSend);
        Assertions.assertEquals(numberOfMessagesToSend * 3, numberOfMessagesProduced, "Number of messages actually sent should match number to send * 3 - as we produced 3 times.");

        long highestSeenMsgIdOnMaster = 0;
        int numberReceivedFromMaster = 0;
        try (Session session = serverSetup.createSession(ServerType.MASTER, false, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = getConsumerDestinationForScheme(messagingScheme, session, address);
            try (MessageConsumer consumer = session.createConsumer(destination)) {
                while (true) {
                    Message msg = consumer.receive(500);
                    if (msg != null) {
                        numberReceivedFromMaster++;
                        long msgId = ((ActiveMQTextMessage) msg).getCoreMessage().getMessageID();
                        if (highestSeenMsgIdOnMaster < msgId) {
                            highestSeenMsgIdOnMaster = msgId;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        assertThat(numberReceivedFromMaster).as("Should consume all messages produced").isEqualTo(numberOfMessagesProduced);
        assertThat(highestSeenMsgIdOnMaster).as("Highest seen message id as consumed on Master should be below Integer.MAX but above one seen on Slave.").isBetween(1L, (long) Integer.MAX_VALUE);
    }
}
