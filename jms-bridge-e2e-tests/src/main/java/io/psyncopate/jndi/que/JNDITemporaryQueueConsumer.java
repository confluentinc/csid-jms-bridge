package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class JNDITemporaryQueueConsumer {

    public static void main(String[] args) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory connectionFactory = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Queue destination= (Queue) ic.lookup("myQueue");

        // Create a connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a session (non-transacted, auto-acknowledge)
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a message consumer
        MessageConsumer consumer = session.createConsumer(destination);

        // Wait for a message
        Message receivedMessage = consumer.receive(30000); // Wait up to 5 seconds

        if (receivedMessage != null && receivedMessage instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) receivedMessage;
            System.out.println("Message received: " + textMessage.getText());

            // Retrieve the temporary queue from the JMSReplyTo field
            Destination replyDestination = receivedMessage.getJMSReplyTo();
            if (replyDestination != null) {
                // Create a message producer to send the reply
                MessageProducer replyProducer = session.createProducer(replyDestination);
                TextMessage replyMessage = session.createTextMessage("Reply from JNDI Consumer!");

                // Send the reply message to the temporary queue
                replyProducer.send(replyMessage);
                System.out.println("Reply sent: " + replyMessage.getText());

                replyProducer.close();
            }
        } else {
            System.out.println("No message received.");
        }

        // Clean up
        consumer.close();
        session.close();
        connection.close();
        ic.close();
    }
}
