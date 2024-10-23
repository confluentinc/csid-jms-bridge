package io.psyncopate;

import io.psyncopate.service.BrokerService;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.constants.MessagingScheme;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

@ExtendWith(GlobalSetup.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JMSQueueTest {
    private static final Logger logger = LogManager.getLogger(JMSQueueTest.class);
    private static final String SHEET_NAME = "JMS Queue Test";

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);

    JMSCommonTest jmsCommonTest;

    @BeforeAll
    public static void setup() throws IOException {
        // Create a unique directory for logs
    }

    @BeforeEach
    public void init() {
        BrokerService brokerService = new BrokerService(GlobalSetup.getServerSetup());
        jmsCommonTest = new JMSCommonTest(brokerService, GlobalSetup.getServerSetup());
    }

    // Helper method to assert the asynchronous result of message sending
    private void assertAsyncResult(int messageSentToBe, int messageSent) {
        Assertions.assertEquals(messageSentToBe, messageSent, "Number of sent and received messages should match.");
    }

    @Test
    @Order(1)
    @Disabled
    @DisplayName("Queue Sample Test for Queue.")
    void sampleTestForQueue() throws Exception {
        jmsCommonTest.sampleTest(MessagingScheme.JMS_ANYCAST, 20);
    }

    @Test
    @Order(1)
    @DisplayName("Queue Master failover after producing, switch to slave, then consume messages.")
    void startServersAndGracefulFailoverForQueue() throws Exception {
        jmsCommonTest.startServersAndGracefulFailover(MessagingScheme.JMS_ANYCAST, 20);
    }

    @Test
    @Order(2)
    @DisplayName("Queue Master failover with forced kill after producer is stopped, switch to slave, then consume messages.")
    void startServersAndForceFailoverWithProducerStopForqueue() throws Exception {
        jmsCommonTest.startServersAndForceFailoverWithProducerStop(MessagingScheme.JMS_ANYCAST, 20);
    }

    @Test
    @Order(3)
    @DisplayName("Queue Master failover with forced kill, switch to slave, then consume messages.")
    void startServersAndFailoverForQueue() throws Exception {
        jmsCommonTest.startServersAndFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(4)
    @DisplayName("Queue Start live server, backup server takes over after live server is killed, with partial and full message consumption.")
    void partialConsumeAndFailoverForQueue() throws Exception {
        jmsCommonTest.partialConsumeAndFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(5)
    @DisplayName("Queue Master failover with two producers, switch to slave, then consume all messages..")
    void parallelProducersWithFailoverForQueue() throws Exception {
        jmsCommonTest.parallelProducersWithFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(6)
    @DisplayName("Queue Master failover with two producers and phased message consumption.")
    void parallelProducersPartialConsumeAndFailoverForQueue() throws Exception {
        jmsCommonTest.parallelProducersPartialConsumeAndFailover(MessagingScheme.JMS_ANYCAST);
    }

//    @Test
//    @DisplayName("Master failover with two consumers for partial and complete message consumption.")
//    void parallelPartialConsumeAndFailover() throws Exception {
//        Assertions.assertTrue(ServerSetup.startMasterServer(), "Master Server should start.");
//        Assertions.assertTrue(ServerSetup.startSlaveServer(), "Slave Server should start.");
//
//        String queueName = Util.getCurrentMethodNameAsAnycastAddress();
//
//        CompletableFuture<Integer> asyncProducerMaster1 = ServerSetup.startJmsProducerAsync(queueName, RoutingType.ANYCAST);
//        CompletableFuture<Integer> asyncConsumerMaster1 = ServerSetup.startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 100L);
//        CompletableFuture<Integer> asyncConsumerMaster2 = ServerSetup.startJmsConsumerAsync(queueName, RoutingType.ANYCAST, 100L);
//
//        Assertions.assertTrue(ServerSetup.killMasterServer(10), "Master Server should stop.");
//        Assertions.assertTrue(ServerSetup.isServerUp(ServerType.SLAVE, 20,1), "Slave Server should be running.");
//        int consumerSlave = ServerSetup.startJmsConsumer(queueName, RoutingType.ANYCAST);
//
//        Assertions.assertTrue(ServerSetup.stopSlaveServer(), "Slave Server should stop.");
//
//        int messageSent=asyncProducerMaster1.get();
//        int messagesReceived= asyncConsumerMaster1.get() + asyncConsumerMaster2.get() + consumerSlave ;
//
//        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
//    }

    @Test
    @Order(7)
    @DisplayName("Queue Master failover with producer restart and full message consumption.")
    void failoverWithResumedProductionAndConsumeForQueue() throws Exception {
        jmsCommonTest.failoverWithResumedProductionAndConsume(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(8)
    @DisplayName("Queue Master and slave failovers with producer restarts and complete message consumption.")
    void dualFailoverWithProducerAndConsumeForQueue() throws Exception {
        jmsCommonTest.dualFailoverWithProducerAndConsume(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(9)
    @DisplayName("Queue Master and slave failovers with producer restarts and phased message consumption.")
    void dualFailoverWithProducerAndStagedConsumptionForQueue() throws Exception {
        jmsCommonTest.dualFailoverWithProducerAndStagedConsumption(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(10)
    @DisplayName("Queue Master and slave failovers with producer, followed by consumer message consumption.")
    void dualKillAndMasterRestartWithMessageConsumptionForQueue() throws Exception {
        jmsCommonTest.dualKillAndMasterRestartWithMessageConsumption(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(11)
    @DisplayName("Queue Master failover, partial consumption, master restart, and complete message consumption.")
    void partialConsumptionWithFailoverAndMasterRestartForQueue() throws Exception {
        jmsCommonTest.partialConsumptionWithFailoverAndMasterRestart(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(12)
    @DisplayName("Queue Master failover, producer start, partial and full message consumption with master restart.")
    void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumptionForQueue() throws Exception {
        //Failed testcase
        jmsCommonTest.failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(13)
    @DisplayName("Queue Master and slave failovers with producer restarts and full message consumption.")
    void tripleFailoverWithProducerRestartsAndFinalConsumptionForQueue() throws Exception {
        jmsCommonTest.tripleFailoverWithProducerRestartsAndFinalConsumption(MessagingScheme.JMS_ANYCAST);
    }
}
