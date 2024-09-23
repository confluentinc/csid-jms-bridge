package org.example.jndi.que.info;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

public class ListQueuesByAddress {

    public static void main(String[] args) throws Exception {

        String brokerUrl = "tcp://jms-bridge-test-vm.psyncopate.io:61616";
        String addressName = "four-test-msg-durable";
        ActiveMQConnectionFactory connectionFactory = ActiveMQJMSClient.createConnectionFactory(brokerUrl,"");
        QueueConnection connection = connectionFactory.createQueueConnection();
        try {
            connection.start();

            // Create a JMS Session
            QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a management queue
            Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

            // Create a QueueRequestor to send requests
            QueueRequestor requestor = new QueueRequestor(session, managementQueue);

            // Create a JMS Message
            Message m = session.createMessage();

            // Set the operation to get the list of queues
            JMSManagementHelper.putOperationInvocation(m, ResourceNames.ADDRESS + addressName, "getQueueNames");

            // Send the request and wait for the response
            Message reply = requestor.request(m);

            // Check if the operation succeeded
            if (JMSManagementHelper.hasOperationSucceeded(reply)) {
                // Get the list of queues
                Object[] queueNames = (Object[]) JMSManagementHelper.getResult(reply);
                for (Object queueName : queueNames) {
                    System.out.println("Queue: " + queueName);
                }
            } else {
                System.out.println("Failed to get queue names.");
            }
        } finally {
            connection.close();
        }
    }
}