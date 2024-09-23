package org.example.jndi.que.info;

import org.apache.activemq.artemis.api.core.management.ResourceNames;

import javax.jms.*;
import javax.naming.*;


import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;


public class ListAddress {
    public static void main(String[] args) throws Exception {
        InitialContext ic = new InitialContext();
        QueueConnectionFactory connectionFactory = (QueueConnectionFactory)ic.lookup("QueueConnectionFactory");
        QueueConnection connection = connectionFactory.createQueueConnection();
        connection.start();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
        QueueRequestor requestor = new QueueRequestor(session, managementQueue);
        Message m = session.createMessage();
        JMSManagementHelper.putOperationInvocation(m, ResourceNames.BROKER, "getAddressNames");
        Message reply = requestor.request(m);
        boolean success = JMSManagementHelper.hasOperationSucceeded(reply);
        if (success) {
            Object[] addresses = (Object[]) JMSManagementHelper.getResult(reply);
            for (Object address : addresses) {
                System.out.println(address);
            }
        }
        connection.close();
    }
}