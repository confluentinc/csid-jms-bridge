package io.confluent.jms.bridge.client;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.constants.Constants;
import javax.jms.*;
import java.util.HashMap;

import static io.confluent.jms.bridge.util.Util.isPortOpen;

public class JMSClientTopic {
    private static final Logger logger = LogManager.getLogger(JMSClientTopic.class);

    public static Connection createConnection(HashMap<String, String> currentNode) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + currentNode.get(Constants.HOST) + ":" + currentNode.get(Constants.APP_PORT));
        return connectionFactory.createConnection();
    }

    public static HashMap<String, Object> createSessionAndTopic(Connection connection, String topicName) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        HashMap<String, Object> sessionTopic = new HashMap<>();
        sessionTopic.put(Constants.SESSION, session);
        sessionTopic.put(Constants.TOPIC, session.createTopic(topicName));
        return sessionTopic;
    }

    public int producerOneTopic(int totalMessage, HashMap<String, String> currentNode, String topicName) throws JMSException {
        Connection connection = createConnection(currentNode);
        connection.start();
        HashMap<String, Object> sessionTopic = createSessionAndTopic(connection, topicName);
        MessageProducer producer = ((Session) sessionTopic.get(Constants.SESSION)).createProducer((Topic) sessionTopic.get(Constants.TOPIC));
        if (isPortOpen (currentNode)) {
            logger.info("Sending {} messages from Producer1 to host {} on port {}.", totalMessage, currentNode.get(Constants.HOST), currentNode.get(Constants.APP_PORT));

            try {
                TextMessage message = ((Session) sessionTopic.get(Constants.SESSION)).createTextMessage("Hello, JMS!");
                for (int i = 0; i < totalMessage; i++) {
                    producer.send(message);
                    //Thread.sleep(200);
                    System.out.println("Message Sent: {}");
                }
                logger.info("Number of messages sent: {}", totalMessage);
                return totalMessage;
            } catch (JMSException e) {
                logger.error("Failed to send messages: {}", e.getMessage());
                throw e;
            }
        } else {
            logger.error("The port {} on host {} is not open. Please check the JMS broker.", currentNode.get(Constants.APP_PORT), currentNode.get(Constants.HOST));
            System.err.println("The port " + currentNode.get(Constants.APP_PORT) + " on host " + currentNode.get(Constants.HOST) + " is not open.");
        }
        return 0;
    }

    public int consumerOneTopic(HashMap<String, String> currentNode, String topicName) throws JMSException {
        Connection connection = createConnection(currentNode);
        connection.setClientID("abc");
        connection.start();
        HashMap<String, Object> sessionTopic = createSessionAndTopic(connection, topicName);
        MessageConsumer consumer = ((Session) sessionTopic.get(Constants.SESSION)).createDurableSubscriber((Topic) sessionTopic.get(Constants.TOPIC), "def");

        if (isPortOpen(currentNode )) {
            logger.info("Consuming messages from Consumer1 to host {} on port {}.", currentNode.get(Constants.HOST), currentNode.get(Constants.APP_PORT));

            try {
                int messagesReceived = 0;
                Message receivedMessage;

                System.out.println("Receiving messages from Consumer1...");
                while ((receivedMessage = consumer.receive(1000)) != null) {
                    System.out.println("Consuming");
                    if (receivedMessage instanceof TextMessage) {
                        messagesReceived++;
                    }
                }
                logger.info("Number of messages received: {}", messagesReceived);
                return messagesReceived;
            } catch (JMSException e) {
                logger.error("Failed to receive messages: {}", e.getMessage());
                throw e;
            }
        } else {
            logger.error("The port {} on host {} is not open. Please check the JMS broker.", currentNode.get(Constants.APP_PORT), currentNode.get(Constants.HOST));
            System.err.println("The port " + currentNode.get(Constants.APP_PORT) + " on host " + currentNode.get(Constants.HOST) + " is not open.");
        }
        return 0;
    }
}
