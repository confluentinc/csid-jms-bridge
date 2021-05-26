/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import javax.jms.JMSException;
import javax.jms.Session;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class JmsFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsFactory.class);
  private volatile Connection connection;

  public synchronized Connection openConnection(
      String url,
      String user) throws Exception {

    if (connection == null) {

      String clientId;
      try {
        InetAddress ip = InetAddress.getLocalHost();
        String hostname = ip.getHostName();
        clientId = hostname + "-" + user;
      } catch (Exception e) {
        clientId = "clientId" + "-" + user;
      }

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(url);
      cf.setConfirmationWindowSize(-1);
      cf.setConsumerMaxRate(-1);
      cf.setProducerWindowSize(-1);
      cf.setProducerMaxRate(-1);
      cf.setUser(user);
      javax.jms.Connection amqConnection = cf.createConnection();
      amqConnection.setClientID(clientId);
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
