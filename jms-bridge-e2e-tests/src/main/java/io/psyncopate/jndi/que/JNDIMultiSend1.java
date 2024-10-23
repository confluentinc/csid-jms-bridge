package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Scanner;

public class JNDIMultiSend1 {

    private static volatile boolean stop = false;

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");
        Queue destination = (Queue) ic.lookup("myQueue");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);

        // Start a thread to listen for user input
        Thread inputThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8);
            System.out.println("Press Enter to stop...");
            scanner.nextLine();
            stop = true;
        });
        inputThread.start();

        for (int i = 1; !stop; i++) {
            String message = "{\"timestamp\":\"" + new Date() + "\",\"message\": Sender 2  Message" + i + "}";
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
            System.out.println("Sent message: " + message);
            Thread.sleep(100); // 1 second delay
        }

        // Clean up
        producer.close();
        session.close();
        connection.close();
        System.out.println("Sender stopped.");
    }
}


