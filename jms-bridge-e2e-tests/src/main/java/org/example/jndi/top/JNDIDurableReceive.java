package org.example.jndi.top;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Scanner;

public class JNDIDurableReceive {

    private static volatile boolean stop = false;

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Topic destination=(Topic)ic.lookup("myTopic");
        Connection connection = cf.createConnection();
        //Connection connection = cf.createConnection("sab","sab123");
        connection.setClientID("durableClientId1");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableConsumer(destination,"durableConsumer1");
        connection.start();
        System.out.println("JMS Receiver Start : ");

        // Start a thread to listen for user input
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Press Enter to stop...");
            scanner.nextLine();
            stop = true;
        });
        inputThread.start();

        for (int i = 1; !stop; i++) {
            TextMessage receivedMessage = (TextMessage) consumer.receive();
            System.out.println("Received msg " + i + ": " + receivedMessage.getText());
            Thread.sleep(100);
        }

        // Clean up
        consumer.close();
        session.close();
        connection.close();
        System.out.println("Receiver stopped.");
    }
}
