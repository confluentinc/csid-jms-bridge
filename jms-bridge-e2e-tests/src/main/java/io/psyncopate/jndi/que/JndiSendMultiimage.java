package io.psyncopate.jndi.que;

import javax.jms.*;
import javax.naming.InitialContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

public class JndiSendMultiimage {

    public static void main(String args[]) throws Exception {
        InitialContext ic = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");
        Queue destination = (Queue) ic.lookup("myQueue");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        connection.start();

        String imagePath = "/Users/ganeshprabhu/gpbase/JavaProjects/JmsKafkaSendReceive/src/main/resources/log4j2.xml"; // Update with the path to your image file

        try {
            File imageFile = new File(imagePath);
            byte[] imageData = readFileToByteArray(imageFile);

            while (true) {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(imageData);
                producer.send(bytesMessage);
                System.out.println("Image sent with timestamp: " + new Date());
                // Optional delay
                // Thread.sleep(1000);
            }
        } finally {
            connection.close();
        }
    }

    private static byte[] readFileToByteArray(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            return data;
        }
    }
}
