/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import static java.lang.String.format;
import static org.apache.commons.text.StringEscapeUtils.escapeJson;

import com.github.rvesse.airline.Channels;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.NotBlank;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

@Command(name = "receive", description = "receive text messages from a JMS topic")
public class ReceiveCommand implements BaseCommand {

  @Inject
  JmsClientOptions jmsClientOptions = new JmsClientOptions();

  @Option(name = "--topic", description = "The topic to receive the text messages from.")
  @Required
  @Once
  String topic;

  @Option(name = "--name", description = "The name of the consumer")
  @Once
  @NotBlank
  String name = "jms-client-consumer";

  @Option(name = "--headers", description = "Show headers")
  boolean headers = false;

  @Option(name = "--bin2text", description = "Treat binary messages as UTF-8 text.")
  boolean binToText = false;

  private final CommandIo io;

  public ReceiveCommand(CommandIo io) {
    this.io = io;
  }

  public ReceiveCommand() {
    this.io = CommandIo.create();
  }

  @Override
  public int execute() throws Exception {
    jmsClientOptions.doWithSession(this::receive);
    return 0;
  }

  private void receive(Session session) throws Exception {
    Topic jmsTopic = session.createTopic(topic);

    try (TopicSubscriber consumer = session.createDurableSubscriber(jmsTopic, name)) {
      Channels.output().println("Receiving messages from " + jmsTopic.toString());

      final String prefix = "  ";
      while (true) {
        Message received = consumer.receive();
        if (received != null) {
          Channels.output().println("\"message\": {");
          if (headers) {
            printHeaders(received, prefix);
          }
          final String body = getBody(received);
          Channels.output().println(prefix + "\"body\": \"" + body + "\"");
          Channels.output().println("}");
        }
      }
    }
  }

  private String getBody(Message msg) {
    try {
      if (msg.isBodyAssignableTo(String.class)) {
        return msg.getBody(String.class);
      }

      if (msg.isBodyAssignableTo(byte[].class) && binToText) {
        return new String(msg.getBody(byte[].class), StandardCharsets.UTF_8);
      }
    } catch (JMSException e) {
      //swallow it up
    }

    return "[Message Type is not Text]";
  }

  private void printHeaders(Message msg, String prefix) {
    final List<String> props = new LinkedList<>();
    try {
      final Enumeration<?> propNameEnum = msg.getPropertyNames();
      while (propNameEnum.hasMoreElements()) {
        final String propName = propNameEnum.nextElement().toString();
        final String propVal = Objects.toString(msg.getObjectProperty(propName));
        props.add(format("\"%s\": \"%s\"", escapeJson(propName), escapeJson(propVal)));
      }
    } catch (JMSException e) {
      Channels.output().println(prefix + "\"header_err\": "
          + "\"Failed to read all headers: " + escapeJson(e.getMessage()) + "\", ");
    }
    Channels.output().println(prefix + "\"headers\": {");
    Channels.output().println(prefix
        + "  "
        + String.join("," + System.lineSeparator() + "  ", props));
    Channels.output().println(prefix + "},");
  }
}
