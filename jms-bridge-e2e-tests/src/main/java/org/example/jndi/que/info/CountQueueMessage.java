package org.example.jndi.que.info;

import javax.jms.*;
import javax.naming.*;
import java.util.Enumeration;

public class CountQueueMessage {

    public static void main(String[] args) {
        try {
            // Set up the initial context using JNDI
            InitialContext ic = new InitialContext();
            ConnectionFactory connectionFactory = (ConnectionFactory)ic.lookup("ConnectionFactory");
            Queue destination = (Queue) ic.lookup("myQueue");
            //Topic destination = (Queue) ic.lookup("MyTopic");

            // Create a connection, session, and consumer
            try (Connection connection = connectionFactory.createConnection();
                 Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 QueueBrowser browser = session.createBrowser(destination)) {

                // Get information about the queue
                QueueMetaData queueMetaData = getQueueMetaData(browser);

                // Print the information
                System.out.println("Queue Name: " + queueMetaData.getQueueName());
                System.out.println("Number of Messages: " + queueMetaData.getNumMessages());
            }
        } catch (NamingException | JMSException e) {
            e.printStackTrace();
        }
    }

    private static QueueMetaData getQueueMetaData(QueueBrowser browser) throws JMSException {
        QueueMetaData metaData = new QueueMetaData();

        Enumeration<?> enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements()) {
            enumeration.nextElement();
            metaData.incrementNumMessages();
        }

        metaData.setQueueName(browser.getQueue().getQueueName());

        return metaData;
    }

    private static class QueueMetaData {
        private String queueName;
        private int numMessages;

        public String getQueueName() {
            return queueName;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        public int getNumMessages() {
            return numMessages;
        }

        public void incrementNumMessages() {
            this.numMessages++;
        }
    }
}

