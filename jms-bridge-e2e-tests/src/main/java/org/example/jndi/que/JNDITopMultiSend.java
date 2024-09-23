package org.example.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Date;

public class JNDITopMultiSend {

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic destination= (Topic)ic.lookup("myTopic");
        //Queue destination= (Queue) ic.lookup("myQueue");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        for (int i=1;;i++) {
            String message = "{\"timestamp\":\"" + new Date() + "\",\"message\": Sender 2 Message" + i + "}";
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
            System.out.println("Sent message: " + message);
            Thread.sleep(200); // 1 second delay
        }
    }
}


