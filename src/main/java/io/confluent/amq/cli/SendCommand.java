/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;

import javax.inject.Inject;
import javax.jms.*;
import java.util.List;
import java.util.Scanner;

@Command(name="send", description="Send text messages to a JMS topic")
public class SendCommand implements Runnable {

    @Inject
    JmsClientOptions jmsClientOptions = new JmsClientOptions();

    @Option(name = {"--topic"}, arity = 1, description = "The topic to send the text message to.")
    String topic;

    @Arguments
    List<String> args;

    @Override
    public void run() {
        try {
            jmsClientOptions.doWithSession(this::send);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void send(Session session) {
        try {

            Topic jmsTopic = session.createTopic(topic);
            MessageProducer producer = session.createProducer(jmsTopic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            Scanner input = new Scanner(System.in);
            System.out.println("Ready to send messages to topic: " + jmsTopic.toString());
            while(true) {
                String line = input.nextLine();

                if("quit".equalsIgnoreCase(line)) {
                    break;
                }

                if(line != null && line.length() > 0) {
                    producer.send(session.createTextMessage(line));
                }
            }

        } catch(Exception e) {
            //ignore
        }
    }
}
