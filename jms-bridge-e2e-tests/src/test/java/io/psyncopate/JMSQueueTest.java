package io.psyncopate;

import io.psyncopate.service.BrokerService;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.constants.MessagingScheme;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

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
    public static void setup() {
        //reset bridge conf to original
        GlobalSetup.getServerSetup().uploadUnchangedConfigFile(true);
        GlobalSetup.getServerSetup().uploadUnchangedConfigFile(false);
        //reset broker xml to original
        GlobalSetup.getServerSetup().uploadUnchangedBrokerXMLFile(true);
    }

    @BeforeEach
    public void init() {
        BrokerService brokerService = new BrokerService(GlobalSetup.getServerSetup());
        jmsCommonTest = new JMSCommonTest(brokerService, GlobalSetup.getServerSetup());
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

    @Test
    @Order(14)
    @DisplayName("With local transaction produce to Master, commit, failover to Slave, and consume from Slave.")
    void verifyLocalTransactionProduceFailoverConsume() throws Exception {
        jmsCommonTest.verifyLocalTransactionProduceFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(15)
    @DisplayName("With local transaction produce to Master, commit, consume from Master")
    void verifyLocalTransactionProduceOnMaster() throws Exception {
        jmsCommonTest.verifyLocalTransactionProduceOnMaster(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(16)
    @DisplayName("Produce to Master, verify consume in transaction from Master")
    void verifyLocalTransactionConsumeOnMaster() throws Exception {
        jmsCommonTest.verifyLocalTransactionConsumeOnMaster(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(17)
    @DisplayName("With local transaction produce to Master, dont commit, failover, verify nothing to consume from Slave")
    public void verifyLocalTransactionProduceOnMasterRollbackOnFailover() throws Exception {
        jmsCommonTest.verifyLocalTransactionProduceOnMasterRollbackOnFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(18)
    @DisplayName("Produce to Master, consume in transaction, don't commit, failover, consume again Slave")
    public void verifyLocalTransactionConsumeOnMasterRollbackOnFailover() throws Exception {
        jmsCommonTest.verifyLocalTransactionConsumeOnMasterRollbackOnFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(19)
    @DisplayName("Produce to master, consume in transaction, commit, failover, verify that messages are cannot be consumed again (as 1st consume committed / acked)")
    public void verifyLocalTransactionConsumeOnMasterCommitOnFailover() throws Exception {
        jmsCommonTest.verifyLocalTransactionConsumeOnMasterCommitOnFailover(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(20)
    @DisplayName("Verify highest used message ID persisted and read back on graceful failover and failback")
    public void verifyMessageIdPersistedAndReadBackOnGracefulFailoverAndFailback() throws Exception {
        jmsCommonTest.verifyMessageIdPersistedAndReadBackOnGracefulFailoverAndFailback(MessagingScheme.JMS_ANYCAST);
    }

    @Test
    @Order(21)
    @DisplayName("Verify highest used message ID is not persisted on abrupt (killed) shutdown and is bumped by Integer max")
    public void verifyMessageIdIsIncreasedCorrectlyOnKillFailoverAndFailback() throws Exception {
        jmsCommonTest.verifyMessageIdIsIncreasedCorrectlyOnKillFailoverAndFailback(MessagingScheme.JMS_ANYCAST);
    }
}
