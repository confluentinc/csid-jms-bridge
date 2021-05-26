/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.mq.perf.TestTime;
import io.confluent.mq.perf.data.DataGenerator;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.confluent.mq.perf.clients.CommonMetrics.MSG_PRODUCER_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_PRODUCER_ERROR_COUNT;

public class JmsTestProducer implements CompletionListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestProducer.class);

  private final String url;
  private final String clientName;
  private final String topic;
  private final DataGenerator dataGenerator;
  private final Duration executionTime;
  private final int messageRate;
  private final TestTime ttime;
  private final AtomicLong messagesPublishedCount = new AtomicLong(0);

  public JmsTestProducer(
      String url,
      String clientName,
      String topic,
      DataGenerator dataGenerator,
      Duration executionTime,
      int messageRate,
      TestTime ttime) {

    this.url = url;
    this.clientName = clientName;
    this.topic = topic;
    this.dataGenerator = dataGenerator;
    this.executionTime = executionTime;
    this.messageRate = messageRate;
    this.ttime = ttime;
  }

  @Override
  public void onCompletion(Message message) {
    messagesPublishedCount.incrementAndGet();
    MSG_PRODUCER_COUNT.inc();
  }

  @Override
  public void onException(Message message, Exception exception) {
    MSG_PRODUCER_ERROR_COUNT.inc();
    LOGGER.error("Published message failed.", exception);
  }

  public void start() {
    messagesPublishedCount.set(0);
    LOGGER.info("Starting JMS Publisher");
    Supplier<byte[]> messageSupplier = dataGenerator.createMessageSupplier();
    JmsFactory jmsFactory = new JmsFactory();
    try (
        JmsFactory.Connection jmsCnxn = jmsFactory.openConnection(url, "jms2jms-producer");
        Session session = jmsCnxn.openSession()) {

      Topic jmsTopic = session.createTopic(this.topic);

      LOGGER.info("JMS publisher producing to topic: {}", jmsTopic);
      try (MessageProducer producer = session.createProducer(jmsTopic)) {
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.setDisableMessageID(true);
        //producer.setDisableMessageTimestamp(true);

        Stopwatch testTimer = Stopwatch.createStarted();
        RateLimiter rateLimiter = RateLimiter.create(messageRate);

        boolean timeRemains = true;
        long startTime = ttime.startTime();
        //long startNano = System.nanoTime();
        int batchSize = 10;
        double msgCount = 0;
        while (timeRemains) {
          rateLimiter.acquire();
          BytesMessage message = session.createBytesMessage();
          message.writeBytes(messageSupplier.get());
          message.setLongProperty("test_start_ms", startTime);
          message.setLongProperty("test_msg_ms", System.currentTimeMillis());
          //synchronous send for now
          //producer.send(message);
          //MSG_PRODUCER_COUNT.inc();
          producer.send(message, this);
          timeRemains = testTimer.elapsed().minus(executionTime).isNegative();
        }

        while (MSG_PRODUCER_ERROR_COUNT.get() + MSG_PRODUCER_COUNT.get() < msgCount) {
          try {
            Thread.sleep(100);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("JMS Publisher has encountered problems and is shutting down", e);
      throw new RuntimeException(e);
    }
    LOGGER.info("JMS Publisher has completed.");
  }

  public long getMessagesPublishedCount() {
    return messagesPublishedCount.get();
  }
}
