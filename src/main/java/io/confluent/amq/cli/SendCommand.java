/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.Channels;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import javax.inject.Inject;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

@Command(name = "send", description = "Send text messages to a JMS topic")
public class SendCommand implements BaseCommand {

  @Inject
  JmsClientOptions jmsClientOptions = new JmsClientOptions();

  @Option(name = {"--topic"}, arity = 1, description = "The topic to send the text message to.")
  String topic;

  @Arguments
  List<String> args;

  @Override
  public int execute() throws Exception {
    jmsClientOptions.doWithSession(this::send);
    return 0;
  }

  private void send(Session session) throws Exception {

    Topic jmsTopic = session.createTopic(topic);
    MessageProducer producer = session.createProducer(jmsTopic);
    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    Scanner input = new Scanner(Channels.input(), StandardCharsets.UTF_8.name());
    Channels.output().println("Ready to send messages to topic: " + jmsTopic.toString());

    while (true) {
      String line = input.nextLine();

      if ("quit".equalsIgnoreCase(line)) {
        break;
      }

      if (line != null && line.length() > 0) {
        producer.send(session.createTextMessage(line));
      }
    }
  }
}
