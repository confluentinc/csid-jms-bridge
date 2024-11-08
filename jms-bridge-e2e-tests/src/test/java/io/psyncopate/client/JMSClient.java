package io.psyncopate.client;

import io.psyncopate.GlobalSetup;
import io.psyncopate.client.Model.QueueAttributes;
import io.psyncopate.util.ConfigLoader;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.RoutingType;
import io.psyncopate.util.constants.ServerType;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.jms.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JMSClient {
    private static final Logger logger = LogManager.getLogger(JMSClient.class);


    private String getConnectionUrl(ServerType serverType) {
        if (serverType == null) {
            return "(" + prepareUrl(true) + "," + prepareUrl(false) + ")";
        }
        return prepareUrl(serverType == ServerType.MASTER);
    }


    private static String prepareUrl(boolean isMaster) {
        ConfigLoader configLoader = GlobalSetup.getConfigLoader();
        try {
            String host = isMaster ? configLoader.getServerMasterHost() : configLoader.getServerSlaveHost();
            int port = Integer.parseInt(isMaster ? configLoader.getServerMasterAppPort() : configLoader.getServerSlaveAppPort());
            if (configLoader.isSslEnabled()) {
                String trustStorePath = null;
                try {
                    trustStorePath = JMSClient.class.getClassLoader().getResource(configLoader.getTrustStoreCertificateName()).getPath();
                } catch (NullPointerException e) {
                    logger.warn("Failed to load truststore certificate from classpath.");
                }
                String trustStorePassword = configLoader.getTrustStorePassword();
                String trustStoreCertificateName = configLoader.getTrustStoreCertificateName();

                // Check if the necessary SSL configurations are present
                if (trustStorePath == null || trustStorePath.isEmpty()) {
                    throw new IOException("TrustStore path is missing in application.properties");
                }
                if (trustStorePassword == null || trustStorePassword.isEmpty()) {
                    throw new IOException("TrustStore password is missing in application.properties");
                }
                if (trustStoreCertificateName == null || trustStoreCertificateName.isEmpty()) {
                    throw new IOException("TrustStore certificate name is missing in application.properties");
                }
                // Construct the URL with SSL enabled
                return "tcp://" + host + ":" + port +
                        "?sslEnabled=true;trustStorePath=" + trustStorePath + trustStoreCertificateName +
                        ";trustStorePassword=" + trustStorePassword;
            } else {
                // Construct the URL without SSL
                return "tcp://" + host + ":" + port;
            }
        } catch (Exception e) {
            // Handle the exception by returning an error message or logging it
            logger.error("Error preparing URL: " + e.getMessage()); // You can log this error instead if preferred
            //return "Error: " + e.getMessage(); // Return error message instead of the URL
        }

        return null;
    }

    public Connection getConnection(ServerType serverType, String clientId) throws JMSException {
        return getConnection(serverType, Optional.ofNullable(clientId));
    }

    public Connection getConnection(ServerType serverType) throws JMSException {
        return getConnection(serverType, Optional.empty());
    }

    public  Connection getConnection(ServerType serverType, Optional<String> clientId) throws JMSException {
        String connectionUrl = getConnectionUrl(serverType);
        logger.debug("Creating connection to host {} .", connectionUrl);
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUrl);
        Connection connection;
        if (GlobalSetup.getConfigLoader().isJaasEnabled() || GlobalSetup.getConfigLoader().isSslEnabled()) {
            connection = connectionFactory.createConnection(GlobalSetup.getConfigLoader().getUsername(), GlobalSetup.getConfigLoader().getPassword());
        } else {
            connection = connectionFactory.createConnection();
        }
        clientId.ifPresent(cId -> {
            try {
                connection.setClientID(cId);
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        connection.start();
        return connection;
    }

    public int produceMessages(ServerType serverType, String queueToBeCreated, RoutingType routingType, int messageCountToBeSent) throws JMSException {
        try (Connection connection = getConnection(serverType)) {
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Destination destination = RoutingType.ANYCAST.equals(routingType) ? session.createQueue(queueToBeCreated) : session.createTopic(queueToBeCreated);
                try (MessageProducer producer = session.createProducer(destination)) {
                    int sentMessageCount = 0;

                    try {
                        TextMessage message = session.createTextMessage("Hello, JMS!");
                        for (; (messageCountToBeSent == -1 || sentMessageCount < messageCountToBeSent); sentMessageCount++) {
                            producer.send(message);
                            logger.debug("Message Sent: {}", message.getText());
                            if (messageCountToBeSent == -1) { //unbounded send - throttle a bit to prevent thousands of messages sent...
                                Util.sleepQuietly(10);
                            }
                        }
                        logger.info("Number of messages sent: {}", sentMessageCount);
                        return sentMessageCount;
                    } catch (JMSException e) {
                        logger.error("Failed to send messages, sent messages count: {}, Error : ", sentMessageCount, e);
                    }
                    return sentMessageCount;
                }
            }
        }
    }

    public Session createSessionWithDefaultConnection(ServerType serverType, boolean transacted, int acknowledgeMode, String clientId) throws JMSException {
        return createSessionWithDefaultConnection(serverType, transacted, acknowledgeMode, Optional.of(clientId));
    }

    public Session createSessionWithDefaultConnection(ServerType serverType, boolean transacted, int acknowledgeMode) throws JMSException {
        return createSessionWithDefaultConnection(serverType, transacted, acknowledgeMode, Optional.empty());
    }

    private Session createSessionWithDefaultConnection(ServerType serverType, boolean transacted, int acknowledgeMode, Optional<String> clientId) throws JMSException {
        Connection connection = getConnection(serverType, clientId);
        return connection.createSession(transacted, acknowledgeMode);
    }

    public int consumeMessages(String queueName, RoutingType routingType, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
        return consumeMessages(null, queueName, routingType, messageCount, sleepInMillis);
    }

    public int consumeMessages(ServerType serverType, String queueToBeCreated, RoutingType routingType, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
        Connection connection = getConnection(serverType);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = RoutingType.ANYCAST.equals(routingType) ? session.createQueue(queueToBeCreated) : session.createTopic(queueToBeCreated);
        MessageConsumer consumer = session.createConsumer(destination);

        int messagesReceived = 0;

        try {

            Message receivedMessage;

            logger.debug("Receiving messages from Consumer...");
            while ((messageCount == -1 || messagesReceived < messageCount) && (receivedMessage = consumer.receive(3000)) != null) {
                logger.trace("Message Received: {}", ((TextMessage) receivedMessage).getText());
                messagesReceived++;
                Thread.sleep(sleepInMillis);
            }
            logger.debug("Number of messages received: {}", messagesReceived);
            return messagesReceived;
        } catch (JMSException | InterruptedException e) {
            logger.error("Failed to receive messages: {}", e.getMessage());
        }
        logger.debug("Consumer closed and received count: {}", messagesReceived);
        return messagesReceived;
    }
    public void produceMessages(MessageProducer producer, Session session, int numberToSend) throws JMSException {
        for (int i = 1; i <= numberToSend; i++) {
            TextMessage message = session.createTextMessage("Produce " + i);
            producer.send(message);
            logger.debug("Message Sent: {}", message.getText());
        }
    }

    public Map<String, QueueAttributes> getAddressQueueInfo(String addressName) throws Exception {
        Connection connection = getConnection(null);
        QueueSession session = (QueueSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
        QueueRequestor requestor = new QueueRequestor(session, managementQueue);

        Map<String, QueueAttributes> queueInfoMap = new HashMap<>();

        // Retrieve queues for the given address
        Message m = session.createMessage();
        JMSManagementHelper.putOperationInvocation(m, ResourceNames.ADDRESS + addressName, "getQueueNames");
        Message reply = requestor.request(m);

        boolean success = JMSManagementHelper.hasOperationSucceeded(reply);
        if (success) {
            Object[] queueNames = (Object[]) JMSManagementHelper.getResult(reply);
            for (Object queueName : queueNames) {
                QueueAttributes attributeMap = setAttributes(session, requestor, queueName);
                queueInfoMap.put(queueName.toString(), attributeMap);
            }
        }

        connection.close();
        return queueInfoMap;
    }

    private static QueueAttributes setAttributes(QueueSession session, QueueRequestor requestor, Object queueName) throws Exception {
        QueueAttributes attributeMap = new QueueAttributes();
        //String[] attributes = {"durable", "consumerCount", "messageCount", "maxConsumers", "scheduledCount", "deliveringCount", "paused", "temporary", "routingType", "autoDeleted"};
        String[] attributes = {
                "durable", "consumerCount", "messageCount", "maxConsumers",
                "scheduledCount", "deliveringCount", "paused", "temporary",
                "routingType", "autoDeleted", "name", "created", "lastMessageTimestamp", "messageExpiration"
        };
        for (String attribute : attributes) {
            Message requestMessage = session.createMessage();
            JMSManagementHelper.putAttribute(requestMessage, "queue." + queueName, attribute);
            Message reply = requestor.request(requestMessage);
            if (JMSManagementHelper.hasOperationSucceeded(reply)) {
                Object result = JMSManagementHelper.getResult(reply, String.class);

                // Set the attribute based on its name
                switch (attribute) {
                    case "durable":
                        attributeMap.setDurable(Boolean.parseBoolean(result.toString()));
                        break;
                    case "consumerCount":
                        attributeMap.setConsumerCount(Integer.parseInt(result.toString()));
                        break;
                    case "messageCount":
                        attributeMap.setMessageCount(Integer.parseInt(result.toString()));
                        break;
                    case "maxConsumers":
                        attributeMap.setMaxConsumers(Integer.parseInt(result.toString()));
                        break;
                    case "scheduledCount":
                        attributeMap.setScheduledCount(Integer.parseInt(result.toString()));
                        break;
                    case "deliveringCount":
                        attributeMap.setDeliveringCount(Integer.parseInt(result.toString()));
                        break;
                    case "paused":
                        attributeMap.setPaused(Boolean.parseBoolean(result.toString()));
                        break;
                    case "temporary":
                        attributeMap.setTemporary(Boolean.parseBoolean(result.toString()));
                        break;
                    case "routingType":
                        attributeMap.setRoutingType(result.toString());
                        break;
                    case "autoDeleted":
                        attributeMap.setAutoDeleted(Boolean.parseBoolean(result.toString()));
                        break;
                    case "name":
                        attributeMap.setName(result.toString());
                        break;
                    case "created":
                        attributeMap.setCreated(Long.parseLong(result.toString())); // Assuming created is a timestamp
                        break;
                    case "lastMessageTimestamp":
                        attributeMap.setLastMessageTimestamp(Long.parseLong(result.toString())); // Assuming timestamp
                        break;
                    case "messageExpiration":
                        attributeMap.setMessageExpiration(Long.parseLong(result.toString())); // Assuming expiration is a timestamp
                        break;
                    // Add handling for any other attributes you may want to include
                }
            }
        }

        return attributeMap;
    }

}
