package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class JNDITemporaryQueueProducer {

    public static void main(String[] args) throws Exception {
        // Set up JNDI properties
        InitialContext ic = new InitialContext();
        ConnectionFactory connectionFactory = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Queue destination= (Queue) ic.lookup("myQueue");

        // Create a connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a session (non-transacted, auto-acknowledge)
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create a message producer
        MessageProducer producer = session.createProducer(destination);

        // Create a temporary queue for receiving the reply
        TemporaryQueue tempQueue = session.createTemporaryQueue();

        // Create a text message
        TextMessage message = session.createTextMessage("Hello from JNDI Producer!");

        // Set the reply-to field to the temporary queue
        message.setJMSReplyTo(tempQueue);

        // Send the message
        producer.send(message);
        System.out.println("Message sent: " + message.getText());

        // Create a consumer for the temporary queue
        MessageConsumer tempConsumer = session.createConsumer(tempQueue);

        // Wait for a reply (synchronous blocking call)
        Message replyMessage = tempConsumer.receive(30000); // Wait up to 5 seconds
        if (replyMessage != null && replyMessage instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) replyMessage;
            System.out.println("Received reply: " + textMessage.getText());
        }

        // Clean up
        producer.close();
        tempConsumer.close();
        session.close();
        connection.close();
        ic.close();
    }
}

