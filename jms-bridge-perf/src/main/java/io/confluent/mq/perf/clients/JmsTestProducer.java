/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.mq.perf.data.DataGenerator;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.confluent.mq.perf.clients.CommonMetrics.MSG_PRODUCER_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_PRODUCER_ERROR_COUNT;

public class JmsTestProducer implements CompletionListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestProducer.class);

  private final JmsFactory jmsFactory;
  private final String topic;
  private final DataGenerator dataGenerator;
  private final Duration executionTime;
  private final int messageRate;
  private final String testId;
  private final AtomicLong messagesPublishedCount = new AtomicLong(0);
  private final boolean useAsync;

  public JmsTestProducer(
      JmsFactory jmsFactory,
      String topic,
      DataGenerator dataGenerator,
      Duration executionTime,
      int messageRate,
      boolean async,
      String testId) {

    this.jmsFactory = jmsFactory;
    this.topic = topic;
    this.dataGenerator = dataGenerator;
    this.executionTime = executionTime;
    this.messageRate = messageRate;
    this.testId = testId;
    this.useAsync = async;
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

  public Long start() {
    messagesPublishedCount.set(0);
    LOGGER.info("Starting JMS Publisher");
    Supplier<byte[]> messageSupplier = dataGenerator.createMessageSupplier();

    try (JMSContext jmsContext = jmsFactory.createContext()) {
      Topic jmsTopic = jmsContext.createTopic(this.topic);
      JMSProducer producer = jmsContext.createProducer();
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

      if (useAsync) {
        producer.setAsync(this);
      }

      LOGGER.info("JMS publisher producing to topic: {}", jmsTopic);

      Stopwatch testTimer = Stopwatch.createStarted();
      RateLimiter rateLimiter = RateLimiter.create(messageRate);

      boolean timeRemains = true;
      int batchSize = 10;
      double msgCount = 0;

      while (timeRemains) {
        rateLimiter.acquire();
        BytesMessage message = jmsContext.createBytesMessage();
        message.writeBytes(messageSupplier.get());
        message.setStringProperty("test_id", testId);
        message.setLongProperty("test_msg_ms", System.currentTimeMillis());
        producer.send(jmsTopic, message);
        if (!useAsync) {
          onCompletion(message);
        }
        timeRemains = testTimer.elapsed().minus(executionTime).isNegative();
      }

      if (useAsync) {
        //wait for async responses from server to finish up
        while (messagesPublishedCount.get() < msgCount) {
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
    return messagesPublishedCount.get();
  }

  public long getMessagesPublishedCount() {
    return messagesPublishedCount.get();
  }
}
