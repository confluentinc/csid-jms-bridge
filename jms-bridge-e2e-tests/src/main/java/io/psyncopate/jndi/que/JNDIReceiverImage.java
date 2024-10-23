package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;
import java.io.FileOutputStream;
import java.io.IOException;

public class JNDIReceiverImage {

        public static void main(String args[]) throws Exception {
                InitialContext ic = new InitialContext();
                ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");
                Queue queue = (Queue) ic.lookup("myQueue"); // Fixed type to Queue
                Connection connection = cf.createConnection();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(queue);
                connection.start();
                System.out.println("JMS Receiver Start: ");

                int i = 1;
                while (true) {
                        // Receive the message
                        BytesMessage receivedMessage = (BytesMessage) consumer.receive();

                        // Read the bytes from the message
                        byte[] imageData = new byte[(int) receivedMessage.getBodyLength()];
                        receivedMessage.readBytes(imageData);

                        // Save the image to a file
                        try (FileOutputStream fos = new FileOutputStream("/files/"+i+".xml")) {
                                fos.write(imageData);
                        }

                        System.out.println(i + " Image received and saved.");
                        i++;

                        // Optional sleep
                        Thread.sleep(100);

                        // Optional break condition
                        // if (i == 30) {
                        //     break;
                        // }
                }

                // Cleanup
        }
}
