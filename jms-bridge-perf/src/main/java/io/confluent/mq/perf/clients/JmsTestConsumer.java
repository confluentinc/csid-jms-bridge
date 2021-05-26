/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.mq.perf.TestTime;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.mq.perf.clients.CommonMetrics.MESSAGE_LATENCY_HIST;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_ERROR_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_SIZE_GAUGE;

public class JmsTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestConsumer.class);

  private final String url;
  private final String clientName;
  private final String topic;
  private final Duration executionTime;
  private final TestTime ttime;
  private final JmsFactory jmsFactory;
  private final LinkedTransferQueue<Message> messageQueue = new LinkedTransferQueue<>();
  private final AtomicLong messagesConsumedCount = new AtomicLong(0);
  private volatile boolean run = true;

  public JmsTestConsumer(
      String url,
      String clientName,
      String topic,
      Duration executionTime,
      TestTime ttime) {

    this.url = url;
    this.clientName = clientName;
    this.topic = topic;
    this.executionTime = executionTime;
    this.ttime = ttime;
    this.jmsFactory = new JmsFactory();
  }

  private void messageListener(Message message) {
    if (messageQueue.offer(message)) {
      MSG_CONSUMED_COUNT.inc();
      if (messagesConsumedCount.incrementAndGet() % 100 == 0) {
        try {
          message.acknowledge();
        } catch (Exception e) {
          LOGGER.error("Failed to ACK messages", e);
        }
      }
    } else {
      LOGGER.warn("Consumer onMessage queue refuses message offer!");
    }
  }

  public Starter createStarter() {
    return new Starter(new CompletableFuture<>(), this::start);
  }

  public void stop() {
    this.run = false;
  }

  private void start(CompletableFuture<Void> readySignal) {
    messagesConsumedCount.set(0);
    LOGGER.info("Starting JMS Consumer");
    try (
        JmsFactory.Connection jmsCnxn = jmsFactory.openConnection(url, "jms2jms");
        Session session = jmsCnxn.openSession()) {

      Topic jmsTopic = session.createTopic(this.topic);

      LOGGER.info("JMS Consumer consuming from topic: {}", jmsTopic);
      try (MessageConsumer consumer = session.createDurableConsumer(jmsTopic, "jms2jms")) {
        consumer.setMessageListener(this::messageListener);
        jmsCnxn.start();
        readySignal.complete(null);

        Message lastMessage = null;
        boolean drain = true;
        while (run || drain) {
          try {
            Message message = messageQueue.poll(30, TimeUnit.SECONDS);
            if (message != null) {
              lastMessage = message;
              recordSample(System.currentTimeMillis(), message);
            } else if (!run) {
              drain = false;
            }
          } catch (InterruptedException e) {
            //ignore
          }
        }
        if (lastMessage != null) {
          lastMessage.acknowledge();
        }
      }
    } catch (Exception e) {
      LOGGER.error("JMS Consumer has encountered problems and is shutting down", e);
      throw new RuntimeException(e);
    }
    LOGGER.info("JMS Consumer has completed.");
  }

  protected void recordSample(
      long consumeTs,
      Message message) {

    try {
      long testStartMs = message.getLongProperty("test_start_ms");
      BytesMessage byteMessage = (BytesMessage) message;

      if (testStartMs == ttime.startTime()) {
        long produceTs = message.getLongProperty("test_msg_ms");

        MESSAGE_LATENCY_HIST.observe(consumeTs - produceTs);

        MSG_CONSUMED_SIZE_GAUGE.set(byteMessage.getBodyLength());

      }

    } catch (Exception e) {
      LOGGER.error("Failed to parse message details.", e);
      MSG_CONSUMED_ERROR_COUNT.inc();
    }
  }

  public long getMessagesConsumedCount() {
    return messagesConsumedCount.get();
  }
}
