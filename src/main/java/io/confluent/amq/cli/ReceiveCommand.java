/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import javax.inject.Inject;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

@Command(name = "receive", description = "receive text messages from a JMS topic")
public class ReceiveCommand implements Runnable {

  @Inject
  JmsClientOptions jmsClientOptions = new JmsClientOptions();

  @Option(name = {
      "--topic"}, arity = 1, description = "The topic to receive the text messages from.")
  String topic;

  @Option(name = {"-n", "--name"}, arity = 1, description = "The name of the consumer")
  String name = "jms-client-consumer";

  @Override
  public void run() {
    try {
      jmsClientOptions.doWithSession(this::receive);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private void receive(Session session) {
    try {

      Topic jmsTopic = session.createTopic(topic);
      try (TopicSubscriber consumer = session.createDurableSubscriber(jmsTopic, name)) {
        System.out.println("Receiving messages from " + jmsTopic.toString());

        while (true) {
          Message received = consumer.receive();
          if (received != null) {
            System.out.println("Body: " + new String(received.getBody(byte[].class)));
          } else {
            System.out.println("No messages received");
          }
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
