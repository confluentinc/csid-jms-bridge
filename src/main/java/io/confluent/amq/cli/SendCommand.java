/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import javax.inject.Inject;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

@Command(name = "send", description = "Send text messages to a JMS topic")
public class SendCommand implements BaseCommand {

  @Inject
  protected JmsClientOptions jmsClientOptions = new JmsClientOptions();

  @Option(name = "--topic", description = "The topic to send the text message to.")
  @Required
  @Once
  protected String topic;

  private final CommandIo io;

  public SendCommand(CommandIo io) {
    this.io = io;
  }

  public SendCommand() {
    this.io = CommandIo.create();
  }

  @Override
  public int execute() throws Exception {
    jmsClientOptions.doWithSession(this::send);
    return 0;
  }

  protected void send(Session session) throws Exception {

    Topic jmsTopic = session.createTopic(topic);
    MessageProducer producer = session.createProducer(jmsTopic);
    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    Scanner input = new Scanner(io.input(), StandardCharsets.UTF_8.name());
    io.output().println("Ready to send messages to topic: " + jmsTopic.toString());

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
