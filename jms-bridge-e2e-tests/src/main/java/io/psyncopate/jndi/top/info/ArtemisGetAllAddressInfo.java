package io.psyncopate.jndi.top.info;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.commons.lang3.StringUtils;

import javax.jms.*;
import javax.naming.InitialContext;

public class ArtemisGetAllAddressInfo {
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
                    getQueueInfo(session, requestor, ResourceNames.ADDRESS + object.toString(), "getBindingNames");

                } else  {
                    System.out.println("\t Queue : "+object);

                    displayAttributes(session,requestor,object);
                }
            }
        }
    }

    private static void displayAttributes(QueueSession session, QueueRequestor requestor, Object queueName) throws Exception {
        Message requestMessage = session.createMessage();
        //String[] attributes = {"durable", "consumerCount", "messageCount","maxConsumers","scheduledCount","deliveringCount","paused","temporary","routingType","subscriptionCount","subscribers"};
        String[] attributes = {
                "durable",
                "consumerCount",
                "messageCount",
                "maxConsumers",
                "scheduledCount",
                "deliveringCount",
                "paused",
                "temporary",
                "address",
                "routingType"
        };
        for (String attribute : attributes) {
            JMSManagementHelper.putAttribute(requestMessage, "queue." + queueName, attribute);
            Message reply = requestor.request(requestMessage);
            if (JMSManagementHelper.hasOperationSucceeded(reply)) {
                System.out.println("\t \t"+ StringUtils.capitalize(attribute)+" \t\t :  "+JMSManagementHelper.getResult(reply, String.class));
            }
        }



    }

}
