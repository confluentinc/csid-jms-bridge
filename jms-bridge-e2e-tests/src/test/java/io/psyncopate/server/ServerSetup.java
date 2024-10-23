package io.psyncopate.server;

import io.psyncopate.util.ConfigLoader;
import io.psyncopate.util.constants.Constants;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.client.JMSClient;
import io.psyncopate.client.KafkaJmsClient;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.RoutingType;
import io.psyncopate.util.constants.ServerType;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static io.psyncopate.util.Util.steps;

public class ServerSetup {
    private final Constants.TestExecutionMode testExecutionMode;
    private static final Logger logger = LogManager.getLogger(ServerSetup.class);
    private final ServerControl serverControl;

    public ServerSetup(ServerControl serverControl, Constants.TestExecutionMode testExecutionMode) {
        this.serverControl = serverControl;
        this.testExecutionMode = testExecutionMode;
    }

    public boolean startMasterServer() {
        boolean isSuccess = serverControl.startServer( true);
        if (isSuccess) {
            Util.addSteps(steps, "Started Master Server");
        }
        return isSuccess;
    }

    public boolean startMasterServer(int delayInSecs) {
        try {
            Thread.sleep(delayInSecs * 1000L);
        } catch (InterruptedException e) {
            logger.info("Delay was interrupted while stopping server", e);
        }
        return startMasterServer();
    }

    public boolean startSlaveServer() {
        boolean isSuccess = serverControl.startServer( false);
        if (isSuccess) {
            Util.addSteps(steps, "Started Slave Server");
        }
        return isSuccess;
    }

    public boolean startSlaveServer(int delayInSecs) {
        try {
            Thread.sleep(delayInSecs * 1000L);
        } catch (InterruptedException e) {
            logger.info("Delay was interrupted while stopping server", e);
        }
        return startSlaveServer();
    }

    public boolean stopMasterServer() {
        boolean isSuccess = serverControl.stopServer( true, false);
        if (isSuccess) {
            Util.addSteps(steps, "Stopped Master Server");
            if (Util.isDownloadLog) {
                serverControl.downloadLog( Util.getCurrentMethodNameByLevel(3), true);
            }
        }
        return isSuccess;
    }


    public boolean stopSlaveServer() {
        boolean isSuccess = serverControl.stopServer( false, false);
        if (isSuccess) {
            Util.addSteps(steps, "Stopped Slave Server");
            if (Util.isDownloadLog) {
                serverControl.downloadLog(Util.getCurrentMethodNameByLevel(3), false);
            }
        }
        return isSuccess;
    }

    public boolean stopSlaveServer(int delayInSecs) {
        try {
            Thread.sleep(delayInSecs * 1000L);
        } catch (InterruptedException e) {
            logger.info("Delay was interrupted while stopping server", e);
        }
        return stopSlaveServer();
    }

    public boolean killSlaveServer() {
        boolean isSuccess = serverControl.stopServer(false, true);
        if (isSuccess) {
            Util.addSteps(steps, "Killed Slave Server");
        }
        return isSuccess;
    }

    public boolean killSlaveServer(int delayInSecs) {
        try {
            Thread.sleep(delayInSecs * 1000L);
        } catch (InterruptedException e) {
            logger.info("Delay was interrupted while stopping server", e);
        }
        return killSlaveServer();
    }

    public boolean killMasterServer() {
        boolean isSuccess = serverControl.stopServer(true, true);
        if (isSuccess) {
            Util.addSteps(steps, "Killed Master Server");
        }
        return isSuccess;
    }

    public boolean killMasterServer(int delayInSecs) {
        try {
            Thread.sleep(delayInSecs * 1000L);
        } catch (InterruptedException e) {
            logger.info("Delay was interrupted while stopping server", e);
        }
        return killMasterServer();
    }

    public int startJmsProducer(Boolean toMaster, String destination, RoutingType routingType, int totalMessage) throws JMSException {
        Util.addSteps(steps, "Started JMS Producer");
        JMSClient jmsClient = new JMSClient();
        int sentMessagesCount = jmsClient.produceMessages(toMaster, destination, routingType, totalMessage);
        if (totalMessage != -1) {
            Util.addSteps(steps, "Stopped JMS Producer");
        }
        return sentMessagesCount;
    }

    public int startJmsProducer(String destination, RoutingType routingType, int totalMessage) throws JMSException {
        Util.addSteps(steps, "Started JMS Producer");
        JMSClient jmsClient = new JMSClient();
        int sentMessagesCount = jmsClient.produceMessages(null, destination, routingType, totalMessage);
        if (totalMessage != -1) {
            Util.addSteps(steps, "Stopped JMS Producer");
        }

        return sentMessagesCount;
    }

    public int startKafkaProducer(HashMap<String, String> server, String destination, int messageToBeSent) throws JMSException {

        Util.addSteps(steps, "Started Kafka Producer");
        int sentMessagesCount = KafkaJmsClient.kafkaProducer(server, destination, messageToBeSent);
        if (messageToBeSent != -1) {
            Util.addSteps(steps, "Stopped Kafka Producer");
        }

        return sentMessagesCount;
    }

    public CompletableFuture<Integer> startJmsProducerAsync(String destination, int messageCountToBeSent, RoutingType routingType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return startJmsProducer(destination, routingType, messageCountToBeSent);
            } catch (JMSException e) {
                logger.error("Failed to send messages asynchronously: {}", e.getMessage());
                return 0;
            }
        });
    }

    public CompletableFuture<Integer> startJmsProducerAsync(String destination, RoutingType routingType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return startJmsProducer(destination, routingType, -1);
            } catch (JMSException e) {
                logger.error("Failed to send messages asynchronously: {}", e.getMessage());
                return 0;
            }
        });
    }

    public int startKafkaConsumer(HashMap<String, String> server, String address, int messageCount) {
        if (messageCount == -1) {
            Util.addSteps(steps, "Started Kafka Consumer to consume all messages");
        } else {
            Util.addSteps(steps, "Started Kafka Consumer to consume few messages");
        }
        int consumedMessagesCount = KafkaJmsClient.kafkaConsumer(server, address, messageCount);
        Util.addSteps(steps, "Stopped Kafka Consumer");
        return consumedMessagesCount;
    }

    public int startJmsConsumer(Boolean toMaster, String destination, RoutingType routingType, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
        JMSClient jmsClient = new JMSClient();
        if (messageCount == -1) {
            Util.addSteps(steps, "Started JMS Consumer to consume all messages");
        } else {
            Util.addSteps(steps, "Started JMS Consumer to consume few messages");
        }
        int consumedMessagesCount = jmsClient.consumeMessages(toMaster, destination, routingType, messageCount, sleepInMillis);
        Util.addSteps(steps, "Stopped JMS Consumer");
        return consumedMessagesCount;
    }

    public int startJmsConsumer(String destination, RoutingType routingType, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
        JMSClient jmsClient = new JMSClient();
        if (messageCount == -1) {
            Util.addSteps(steps, "Started JMS Consumer to consume all messages");
        } else {
            Util.addSteps(steps, "Started JMS Consumer to consume few messages");
        }
        int consumedMessagesCount = jmsClient.consumeMessages(destination, routingType, messageCount, sleepInMillis);
        Util.addSteps(steps, "Stopped JMS Consumer");
        return consumedMessagesCount;
    }

    public CompletableFuture<Integer> startJmsConsumerAsync(String destination, RoutingType routingType, int totalMessage, Long sleepInMillis) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return startJmsConsumer(destination, routingType, totalMessage, sleepInMillis);
            } catch (JMSException | InterruptedException e) {
                logger.error("Failed to send messages asynchronously: {}", e.getMessage());
                return 0;
            }
        });
    }

    public CompletableFuture<Integer> startJmsConsumerAsync(String destination, RoutingType routingType, Long sleepInMillis) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return startJmsConsumer(destination, routingType, sleepInMillis);
            } catch (JMSException | InterruptedException e) {
                logger.error("Failed to consume messages asynchronously: {}", e.getMessage());
                return 0;
            }
        });
    }

    public int startJmsConsumer(String destination, RoutingType routingType, Long sleepInMillis) throws JMSException, InterruptedException {
        return startJmsConsumer(destination, routingType, -1, sleepInMillis);
    }

    public int startJmsConsumer(String destination, RoutingType routingType) throws JMSException, InterruptedException {
        return startJmsConsumer(destination, routingType, -1, 0L);
    }

    /**
     * Check if the server is up
     *
     * @param serverType             The type of server to check
     * @param retryIntervalInSeconds The interval in seconds to wait before retrying
     * @param maxRetries             The maximum number of retries
     * @return true if the server is up, false otherwise
     */
    public boolean isServerUp(ServerType serverType, int retryIntervalInSeconds, int maxRetries) {
        return serverControl.isServerUp(serverType, retryIntervalInSeconds, maxRetries);
    }

    public boolean downloadLog(String testcaseName, boolean isMaster) {
        return serverControl.downloadLog(testcaseName,isMaster);
    }

    public ConfigLoader getConfigLoader(){
        return serverControl.getConfigLoader();
    }

    public void resetKafkaAndLocalState() {
        serverControl.resetKafkaAndLocalState();
    }

    public void resetInstances() {
        serverControl.resetInstances();
    }
}
