package io.psyncopate.jndi.jaas;

import javax.jms.*;
import javax.naming.InitialContext;

public class JndiJAASSend {
    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic orderTopic= (Topic)ic.lookup("MyTopic");
        Connection connection = cf.createConnection("jcp","jcp123");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(orderTopic);
        connection.start();
        TextMessage message = session.createTextMessage("Hi from JMS 6");
        producer.send(message);
        System.out.println("message is : "+message.getText());
    }
}
