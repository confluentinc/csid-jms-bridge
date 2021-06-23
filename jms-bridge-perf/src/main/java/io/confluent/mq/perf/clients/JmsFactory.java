/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import javax.jms.JMSException;
import javax.jms.Session;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class JmsFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsFactory.class);
  private volatile Connection connection;

  public synchronized Connection getConnection() {
    if (this.connection == null) {
      throw new IllegalStateException(
          "Connection has not been established yet, please call openConnection first");
    }
    return connection;
  }

  public synchronized Connection openConnection(String url) throws Exception {

    if (connection == null) {

      String hostname;
      try {
        InetAddress ip = InetAddress.getLocalHost();
        hostname = ip.getHostName();
      } catch (Exception e) {
        InetAddress ip = InetAddress.getLocalHost();
        hostname = ip.getHostAddress();
      }

      ActiveMQConnectionFactory cf = ActiveMQJMSClient
          .createConnectionFactory(url, hostname + "-cnxn");
      int size = 1024 * 1024 * 10;
      cf.setConsumerWindowSize(size * 2);
      cf.setConsumerMaxRate(size * 2);
      cf.setProducerWindowSize(size);
      cf.setProducerMaxRate(size);
      javax.jms.Connection amqConnection = cf.createConnection();
      amqConnection.setClientID(hostname + "-id");
      connection = new Connection(amqConnection);
    }

    return connection;
  }

  public static class Connection implements AutoCloseable {

    private final javax.jms.Connection jmsConnection;

    public Connection(javax.jms.Connection jmsConnection) {
      this.jmsConnection = jmsConnection;
    }

    public void start() throws JMSException {
      jmsConnection.start();
    }

    public Session openSession() throws Exception {
      return openSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    public Session openSession(boolean transacted, int ackMode) throws Exception {
      return jmsConnection.createSession(transacted, ackMode);
    }

    @Override
    public void close() {
      try {
        jmsConnection.close();
      } catch (JMSException e) {
        //try again
        try {
          jmsConnection.close();
        } catch (JMSException e2) {
          LOGGER.error("Failed to close JMS connection", e2);
        }
      }
    }
  }
}
