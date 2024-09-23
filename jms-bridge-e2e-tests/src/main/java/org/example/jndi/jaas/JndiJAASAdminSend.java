package org.example.jndi.jaas;

import javax.jms.*;
import javax.naming.InitialContext;

public class JndiJAASAdminSend {
    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic orderTopic= (Topic)ic.lookup("MyTopic");
        Connection connection = cf.createConnection("theAdmin","theAdmin123");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(orderTopic);
        connection.start();
        TextMessage message = session.createTextMessage("Hi from JMS 4");
        producer.send(message);
        System.out.println("message is : "+message.getText());
    }
}
