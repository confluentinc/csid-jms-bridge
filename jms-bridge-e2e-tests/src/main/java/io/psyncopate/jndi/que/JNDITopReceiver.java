package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;

public class JNDITopReceiver {

    public static void main(String args[]) throws Exception {
            InitialContext ic = new InitialContext();
            ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
            Topic destination=(Topic)ic.lookup("myTopic");
            //Queue destination=(Queue) ic.lookup("myQueue");
            Connection connection = cf.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(destination);
            connection.start();
            System.out.println("JMS Receiver Start : ");
            for(int i=1;;i++) {
                    TextMessage receivedMessage = (TextMessage) consumer.receive();
                    System.out.println("Received msg "+i +": "+ receivedMessage.getText());
            }
    }
}
