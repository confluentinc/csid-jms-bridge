package org.example.jndi.jaas;

import javax.jms.*;
import javax.naming.InitialContext;

public class JndiJAASAdminReceive {
    public static void main(String[] args) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic topic=(Topic)ic.lookup("MyTopic");
        Connection connection = cf.createConnection("theAdmin","theAdmin123");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(topic);
        connection.start();
        System.out.println("JMS Receiver Start : ");
        while(true) {
            TextMessage receivedMessage = (TextMessage) consumer.receive();
            System.out.println("Got order: " + receivedMessage.getText());
        }
    }
}
