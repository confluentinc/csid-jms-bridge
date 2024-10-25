package io.psyncopate;

import io.psyncopate.service.BrokerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.util.JBTestWatcher;
import io.psyncopate.util.constants.MessagingScheme;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
//@Disabled
@ExtendWith(GlobalSetup.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JMSTopicTest {
    private static final Logger logger = LogManager.getLogger(JMSTopicTest.class);
    private static final String SHEET_NAME = "JMS Topic Test";

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);

    private static Integer topicCount = 0;

    JMSCommonTest jmsCommonTest;
    @BeforeAll
    public static void setup()  {
        //reset bridge conf to original
        GlobalSetup.getServerSetup().uploadUnchangedConfigFile(true);
        GlobalSetup.getServerSetup().uploadUnchangedConfigFile(false);

        //Need to update and upload broker.xml with definitions for Topics (Multicast) for them to be persistent without consumers / subscriptions for Master node only.
        GlobalSetup.getServerSetup().updateBrokerXMLFile(true);
    }

    @BeforeEach
    public void init(){
        BrokerService brokerService = new BrokerService(GlobalSetup.getServerSetup());
        jmsCommonTest = new JMSCommonTest(brokerService, GlobalSetup.getServerSetup());
    }

    // Helper method to assert the asynchronous result of message sending
    private void assertAsyncResult(int messageSentToBe, int messageSent) {
        Assertions.assertEquals(messageSentToBe, messageSent, "Number of sent and received messages should match.");
    }

    @Test
    @Disabled
    @DisplayName("Topic Sample Test for Queue.")
    void sampleTestForTopic() throws Exception {
        jmsCommonTest.sampleTest(MessagingScheme.JMS_MULTICAST, 20);
    }

    @Test
    @DisplayName("Topic Master failover after producing, switch to slave, then consume messages..")
    void startServersAndGracefulFailoverForTopic() throws Exception {
        jmsCommonTest.startServersAndGracefulFailover(MessagingScheme.JMS_MULTICAST, 20);
    }

    @Test
    @DisplayName("Topic Master failover with forced kill after producer is stopped, switch to slave, then consume messages.")
    void startServersAndForceFailoverWithProducerStopForTopic() throws Exception {
        jmsCommonTest.startServersAndForceFailoverWithProducerStop(MessagingScheme.JMS_MULTICAST, 20);
    }

    @Test
    @DisplayName("Topic Master failover with forced kill, switch to slave, then consume messages.")
    void startServersAndFailoverForTopic() throws Exception {
        jmsCommonTest.startServersAndFailover(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Start live server, backup server takes over after live server is killed, with partial and full message consumption.")
    void partialConsumeAndFailoverForTopic() throws Exception {
        jmsCommonTest.partialConsumeAndFailover(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master failover with two producers, switch to slave, then consume all messages..")
    void parallelProducersWithFailoverForTopic() throws Exception {
        jmsCommonTest.parallelProducersWithFailover(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master failover with two producers and phased message consumption.")
    void parallelProducersPartialConsumeAndFailoverForTopic() throws Exception {
        jmsCommonTest.parallelProducersPartialConsumeAndFailover(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master failover with producer restart and full message consumption.")
    void failoverWithResumedProductionAndConsumeForTopic() throws Exception {
        jmsCommonTest.failoverWithResumedProductionAndConsume(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master and slave failovers with producer restarts and complete message consumption.")
    void dualFailoverWithProducerAndConsumeForTopic() throws Exception {
        jmsCommonTest.dualFailoverWithProducerAndConsume(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master and slave failovers with producer restarts and phased message consumption.")
    void dualFailoverWithProducerAndStagedConsumptionForTopic() throws Exception {
        jmsCommonTest.dualFailoverWithProducerAndStagedConsumption(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master and slave failovers with producer, followed by consumer message consumption.")
    void dualKillAndMasterRestartWithMessageConsumptionForTopic() throws Exception {
        jmsCommonTest.dualKillAndMasterRestartWithMessageConsumption(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master failover, partial consumption, master restart, and complete message consumption.")
    void partialConsumptionWithFailoverAndMasterRestartForTopic() throws Exception {
        jmsCommonTest.partialConsumptionWithFailoverAndMasterRestart(MessagingScheme.JMS_MULTICAST);
    }


    @Test
    @DisplayName("Topic Master failover, producer start, partial and full message consumption with master restart.")
    void failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumptionForTopic() throws Exception {
        jmsCommonTest.failoverWithProducerOnSlaveAndMasterRestartWithPhasedConsumption(MessagingScheme.JMS_MULTICAST);
    }

    @Test
    @DisplayName("Topic Master and slave failovers with producer restarts and full message consumption.")
    void tripleFailoverWithProducerRestartsAndFinalConsumptionForTopic() throws Exception {
        jmsCommonTest.tripleFailoverWithProducerRestartsAndFinalConsumption(MessagingScheme.JMS_MULTICAST);
    }
}
