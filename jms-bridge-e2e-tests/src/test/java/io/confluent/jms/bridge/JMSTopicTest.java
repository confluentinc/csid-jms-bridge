package io.confluent.jms.bridge;

import io.confluent.jms.bridge.server.ServerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.JBTestWatcher;
import io.confluent.jms.bridge.util.TestCaseUtil;
import io.confluent.jms.bridge.util.Util;
import io.confluent.jms.bridge.util.constants.AddressScheme;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JMSTopicTest {
    private static final Logger logger = LogManager.getLogger(JMSTopicTest.class);
    private static final String SHEET_NAME = "JMS Topic Test";

    @RegisterExtension
    JBTestWatcher jbTestWatcher = new JBTestWatcher(SHEET_NAME);

    private static Integer topicCount = 0;

    @BeforeAll
    public static void setup() throws IOException, InterruptedException {
        // Create a unique directory for logs

    }

    public String getQueueName() {
        topicCount++;
        return Util.CONFIG_LOADER.getQueueName() + topicCount;
    }

    @Test
    @DisplayName("Test message production to live server")
    void testCase1() throws Exception {
        String testCaseName = "Test message production to live server";
        logger.info(testCaseName);

        Assertions.assertTrue(ServerConfig.ServerSetup.startMasterServer(), "Master Server should start successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.startSlaveServer(), "Slave Server should start successfully.");

        String queueName = getQueueName();
        int messageSentToBe = 5;
        int messageSent = ServerConfig.ServerSetup.startJmsProducer(Util.getMasterServer(), queueName , messageSentToBe, AddressScheme.ANYCAST);
        Assertions.assertEquals(messageSentToBe, messageSent, "Number of sent and received messages should match.");
        int messagesReceived= ServerConfig.ServerSetup.startJmsConsumer(queueName);
        //messagesReceived = 3;
        Assertions.assertTrue(ServerConfig.ServerSetup.stopMasterServer(), "Master Server should stop successfully.");
        Assertions.assertTrue(ServerConfig.ServerSetup.stopSlaveServer(), "Slave Server should stop successfully.");

        // Determine if the test passed
        boolean testPassed = TestCaseUtil.compareValues(messageSent, messagesReceived);
        logger.info("Test passed: {}", testPassed);

        logger.info("Test passed successfully.");
        // Log the test result to the Excel workbook
        //ExcelUtil.logTestCaseResult(SHEET_NAME, testCaseName, steps.toString(), testPassed);
        Assertions.assertEquals(messageSent, messagesReceived, "Number of sent and received messages should match.");
    }

}
