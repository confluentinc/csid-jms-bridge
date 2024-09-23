package io.confluent.jms.bridge.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.activemq.artemis.jms.client.ActiveMQQueueConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.ClientUtil;
import io.confluent.jms.bridge.util.constants.Constants;

import javax.jms.*;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.jms.bridge.util.Util.closeQuietly;

public class JMSClient {
    private static final Logger logger = LogManager.getLogger(JMSClient.class);

    public static ConnectionData createConnection(String connectionUrl, String queueName) throws JMSException {
        logger.debug("Creating connection to host {} on for queue {}.", connectionUrl, queueName);
        ActiveMQQueueConnectionFactory connectionFactory = new ActiveMQQueueConnectionFactory(connectionUrl);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession();
        Queue queue = session.createQueue(queueName);
        return new ConnectionData(connectionFactory, connection, session, queue);
    }

    @SneakyThrows
    public int produceMessages(HashMap<String, String> currentNode, int messageCountToBeSent, String queueToBeCreated) {
        MessageProducer producer = null;
        try (ConnectionData connectionData = createConnection(ClientUtil.getConnectionUrl(currentNode), queueToBeCreated)){
            Session session = connectionData.getSession();
            Queue queue = connectionData.getQueue();
            producer = session.createProducer(queue);
            if (currentNode == null) {
                currentNode = ClientUtil.getActiveNode();
            }
            int sentMessageCount = 0;
            logger.debug("Sending {} messages from Producer one to host {} on port {}.", messageCountToBeSent, currentNode.get(Constants.HOST), currentNode.get(Constants.APP_PORT));
            try {
                while (messageCountToBeSent == -1 || sentMessageCount < messageCountToBeSent) {
                    TextMessage message = session.createTextMessage(String.format("%s %d", "Hello ", sentMessageCount));
                    producer.send(message);
                    sentMessageCount++;
                    Thread.sleep(10);
                }
                logger.info("Number of messages sent: {}", sentMessageCount);
                return sentMessageCount;
            } catch (JMSException e) {
                logger.error("Failed to send messages: {}", e.getMessage());
            }
            logger.info("Number of messages sent: {}", sentMessageCount);
            return sentMessageCount;
        } finally {
            closeQuietly(producer);
        }
    }

    public int produceMessagesWithCompletionWaiting(HashMap<String, String> currentNode, int messageCountToBeSent, String queueToBeCreated) throws JMSException {
        MessageProducer producer = null;
        try (ConnectionData connectionData = createConnection(ClientUtil.getConnectionUrl(currentNode), queueToBeCreated)){
            Session session = connectionData.getSession();
            Queue queue = connectionData.getQueue();
            producer = session.createProducer(queue);
            if (currentNode == null) {
                currentNode = ClientUtil.getActiveNode();
            }

            final AtomicInteger sentMessageCount = new AtomicInteger(0);
            logger.debug("Sending {} messages from Producer one to host {} on port {}.", messageCountToBeSent, currentNode.get(Constants.HOST), currentNode.get(Constants.APP_PORT));
            final AtomicBoolean error = new AtomicBoolean(false);
            try {
                TextMessage message = session.createTextMessage("Hello, JMS!");
                while (!error.get() && (messageCountToBeSent == -1 || sentMessageCount.get() < messageCountToBeSent)) {
                    CountDownLatch sendLatch = new CountDownLatch(1);
                    producer.send(message, new CompletionListener() {
                        @Override
                        public void onCompletion(Message message) {
                            sentMessageCount.incrementAndGet();
                            sendLatch.countDown();
                        }

                        @Override
                        public void onException(Message message, Exception exception) {
                            logger.error("Failed to send message: {}", exception.getMessage());
                            error.set(true);
                            sendLatch.countDown();
                        }
                    });
                    sendLatch.await();
                    logger.debug("Message Sent: {}", message.getText());
                }
                logger.info("Number of messages sent: {}", sentMessageCount);
                return sentMessageCount.get();
            } catch (JMSException | InterruptedException e) {
                logger.error("Failed to send messages: {}", e.getMessage());
            }

            logger.info("Number of messages sent: {}", sentMessageCount);
            return sentMessageCount.get();
        } finally {
            closeQuietly(producer);
        }
    }

    public int consumeMessages(HashMap<String, String> currentNode, String queueName, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
        MessageConsumer consumer = null;

        try (ConnectionData connectionData = createConnection(ClientUtil.getConnectionUrl(currentNode), queueName)){
            Session session = connectionData.getSession();
            Queue queue = connectionData.getQueue();
            consumer = session.createConsumer(queue);

            if (currentNode == null) {
                currentNode = ClientUtil.getActiveNode();
            }
            int messagesReceived = 0;
            logger.debug("Consuming messages from Consumer to host {} on port {}.", currentNode.get(Constants.HOST), currentNode.get(Constants.APP_PORT));

            try {

                Message receivedMessage;

                logger.debug("Receiving messages from Consumer...");
                while ((messageCount == -1 || messagesReceived < messageCount) && (receivedMessage = consumer.receive(3000)) != null) {
                    logger.trace("Message Received: {}", ((TextMessage) receivedMessage).getText());
                    messagesReceived++;
                    if(sleepInMillis>0) {
                        Thread.sleep(sleepInMillis);
                    }
                }
                logger.debug("Number of messages received: {}", messagesReceived);
                return messagesReceived;
            } catch (JMSException | InterruptedException e) {
                logger.error("Failed to receive messages: {}", e.getMessage());
                return messagesReceived;
            }
        } finally {
            closeQuietly(consumer);
        }
    }



    @AllArgsConstructor
    @FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
    @Getter
    public static class ConnectionData implements AutoCloseable {
        ActiveMQQueueConnectionFactory connectionFactory;
        Connection connection;
        Session session;
        Queue queue;

        @Override
        public void close() {
            closeQuietly(session);
            closeQuietly(connection);
            closeQuietly(connectionFactory);
        }
    }
}
