/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import java.net.InetAddress;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;

public class JmsClientOptions {

  @Option(name = "--url", description = "The connection URL to the broker")
  @Once
  protected String brokerUrl = "tcp://localhost:61616";

  @Option(name = "--client-id", description = "Specify the client id to use for the connection")
  @Once
  protected String clientId;

  public Connection openConnection() throws Exception {
    if (clientId == null) {
      String uuid = UUID.randomUUID().toString();
      try {
        InetAddress ip = InetAddress.getLocalHost();
        String hostname = ip.getHostName();
        clientId = hostname + "-" + uuid;
      } catch (Exception e) {
        clientId = uuid;
      }
    }

    ConnectionFactory cf = ActiveMQJMSClient
        .createConnectionFactory(brokerUrl, "jms-client-cli");
    Connection amqConnection = cf.createConnection();
    amqConnection.setClientID(clientId);
    amqConnection.start();
    return amqConnection;
  }

  public Session openSession(Connection connection) throws Exception {
    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  public void doWithSession(SessionAction sessionAction) throws Exception {
    try (Connection connection = openConnection()) {
      try (Session session = openSession(connection)) {
        sessionAction.withSession(session);
      }
    }
  }

  @FunctionalInterface
  interface SessionAction {

    void withSession(Session session) throws Exception;
  }
}
