package io.confluent.amq.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Option;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;

import javax.inject.Inject;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.function.Consumer;

public class JMSClientOptions {

    @Option(name = { "-U", "--url"}, arity = 1, description = "The connection URL to the broker")
    protected String brokerUrl = "tcp://localhost:61616";

    @Option(name = { "--client-id"}, arity = 1, description = "The client id to use for the connection")
    protected String clientId;

    public Connection openConnection() throws Exception {
       if(clientId == null) {
           String uuid = UUID.randomUUID().toString();
           try {
               InetAddress ip = InetAddress.getLocalHost();
               String hostname = ip.getHostName();
               clientId = hostname + "-" + uuid;
           } catch(Exception e) {
               clientId = uuid;
           }
       }

        ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory(brokerUrl, "jms-client-cli");
        Connection amqConnection = cf.createConnection();
        amqConnection.setClientID(clientId);
        amqConnection.start();
        return amqConnection;
    }

    public Session openSession(Connection connection) throws Exception {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void doWithSession(Consumer<Session> sessionConsumer) throws Exception {
        try (Connection connection = openConnection()) {
            try (Session session = openSession(connection)) {
                sessionConsumer.accept(session);
            }
        }
    }
}
