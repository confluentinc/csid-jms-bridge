package io.confluent.amq;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

import java.util.Properties;

public class ConfluentAmqPoc {

    public static void main(String ...args) throws Exception {
       Properties kafkaProps = new Properties();

       ConfluentEmbeddedActiveMQ embeddedAmqServer = new ConfluentEmbeddedActiveMQ.Builder(kafkaProps).build();

       Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           try {
               embeddedAmqServer.stop();
           } catch(Exception e) {
               throw new RuntimeException(e);
           }
       }));

       embeddedAmqServer.start();

        ServerLocator serverLocator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
        ClientSessionFactory factory =  serverLocator.createSessionFactory();

        ClientSession session = factory.createSession();


        session.createAddress(new SimpleString("example2"), RoutingType.MULTICAST, true);
        session.createQueue(new QueueConfiguration("example").setAddress("example2").setDurable(true));

        ClientProducer producer = session.createProducer("example2");

        ClientMessage message = session.createMessage(true);

        message.getBodyBuffer().writeString("Hello");

        producer.send(message);

        session.start();

        ClientConsumer consumer = session.createConsumer("example");

        ClientMessage msgReceived = consumer.receive();

        System.out.println("message = " + msgReceived.getBodyBuffer().readString());

        consumer.close();
        session.deleteQueue("example");

        session.close();
        embeddedAmqServer.stop();
    }
}
