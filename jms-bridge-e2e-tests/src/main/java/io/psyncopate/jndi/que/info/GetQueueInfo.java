package io.psyncopate.jndi.que.info;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;

import javax.jms.*;
import javax.naming.InitialContext;

public class GetQueueInfo {
    public static void main(String[] args) throws Exception {

        InitialContext ic = new InitialContext();
        QueueConnectionFactory connectionFactory = (QueueConnectionFactory)ic.lookup("ConnectionFactory");
        QueueConnection connection = connectionFactory.createQueueConnection();
        connection.start();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
        QueueRequestor requestor = new QueueRequestor(session, managementQueue);
        getQueueInfo(session, requestor, ResourceNames.BROKER, "getAddressNames" );
        connection.close();
    }

    private static void getQueueInfo(QueueSession session, QueueRequestor requestor, String resourceName, String operationName) throws Exception {
        Message m = session.createMessage();
        JMSManagementHelper.putOperationInvocation(m, resourceName, operationName);
        Message reply = requestor.request(m);
        boolean success = JMSManagementHelper.hasOperationSucceeded(reply);
        if (success) {
            Object[] objects = (Object[]) JMSManagementHelper.getResult(reply);
            for (Object object : objects) {
                if (resourceName.equals("broker")) {
                    System.out.println("Address : "+object);
                    getQueueInfo(session, requestor, ResourceNames.ADDRESS + object.toString(), "getQueueNames");
                } else {
                    System.out.println("\t Queue : "+object);
                }
            }
        }
    }
}
