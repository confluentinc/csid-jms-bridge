package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;

public class JNDISendOne {

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Queue destination= (Queue) ic.lookup("myQueue");
        //Topic destination= (Topic)ic.lookup("MyTopic");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        connection.start();
        TextMessage message = session.createTextMessage("Hi from JMS from 1 ");
        producer.send(message);
        System.out.println("message is : "+message.getText());
    }
}


