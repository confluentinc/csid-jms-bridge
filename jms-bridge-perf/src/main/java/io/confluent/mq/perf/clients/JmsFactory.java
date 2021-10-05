/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Random;

public class JmsFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsFactory.class);

  private final boolean useAmqp;
  private final String url;
  private final Random rand = new Random();

  public JmsFactory(String url, boolean useAmqp) {
    this.url = url;
    this.useAmqp = useAmqp;
  }

  private Hashtable amqpProperties() {
    Hashtable props = new Hashtable();
    props.put("java.naming.factory.initial", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
    props.put("connectionFactory.myFactoryLookup", this.url);

    return props;
  }

  private Hashtable amqCoreProperties() {
    Hashtable props = new Hashtable();
    props.put("java.naming.factory.initial",
        "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
    props.put("connectionFactory.myFactoryLookup", this.url);

    return props;
  }

  private synchronized ConnectionFactory getCnxnFactory()
      throws Exception {

    Hashtable jndiProps = useAmqp ? amqpProperties() : amqCoreProperties();
    InitialContext initialContext = new InitialContext(jndiProps);
    ConnectionFactory cnxnFactory = (ConnectionFactory) initialContext.lookup("myFactoryLookup");
    return cnxnFactory;
  }

  public JMSContext createContext() throws Exception {
    return getCnxnFactory().createContext(JMSContext.CLIENT_ACKNOWLEDGE);
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
