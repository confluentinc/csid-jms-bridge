package org.example.jndi.top;

import javax.jms.*;
import javax.naming.InitialContext;

public class JNDISendOne {

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic orderTopic= (Topic)ic.lookup("myTopic");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(orderTopic);
        connection.start();
        TextMessage message = session.createTextMessage("Hi from JMS from 2 ");
        producer.send(message);
        System.out.println("message is : "+message.getText());
    }
}


