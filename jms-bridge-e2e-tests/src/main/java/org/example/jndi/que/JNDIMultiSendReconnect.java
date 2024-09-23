package org.example.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Date;
import java.util.Scanner;

public class JNDIMultiSendReconnect {

    private static volatile boolean stop = false;

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
        Queue destination= (Queue) ic.lookup("myQueue");

        // Start a thread to listen for user input
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Press Enter to stop...");
            scanner.nextLine();
            stop = true;
        });
        inputThread.start();

        while (!stop) {
            try {
                sendMessages(ic, cf, destination);
            } catch (Exception e) {
                System.err.println("Connection failed, retrying in 5 seconds...");
                Thread.sleep(5000); // Wait for 5 seconds before retrying
            }
        }

        System.out.println("Sender stopped.");
    }

    private static void sendMessages(InitialContext ic, ConnectionFactory cf, Queue destination) throws Exception {
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);

        for (int i = 1; !stop; i++) {
            String message = "{\"timestamp\":\"" + new Date() + "\",\"message\": Sender 1  Message" + i + "}";
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
            System.out.println("Sent message: " + message);
            Thread.sleep(100); // 1 second delay
        }

        // Clean up
        producer.close();
        session.close();
        connection.close();
    }
}