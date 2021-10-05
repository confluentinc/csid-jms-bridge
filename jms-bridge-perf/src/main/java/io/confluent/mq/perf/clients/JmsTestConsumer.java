/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.base.Stopwatch;
import javax.jms.BytesMessage;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.confluent.mq.perf.clients.CommonMetrics.MESSAGE_LATENCY_HIST;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_ERROR_COUNT;
import static io.confluent.mq.perf.clients.CommonMetrics.MSG_CONSUMED_SIZE_GAUGE;

public class JmsTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestConsumer.class);

  private final JmsFactory jmsFactory;
  private final String topic;
  private final String queue;
  private final Duration executionTime;
  private final String testId;
  private final CompletableFuture<Void> initCompleted = new CompletableFuture<>();
  private volatile boolean run = true;

  public JmsTestConsumer(
      JmsFactory jmsFactory,
      String topic,
      String queue,
      Duration executionTime,
      String testId) {

    this.jmsFactory = jmsFactory;
    this.topic = topic;
    this.queue = queue;
    this.executionTime = executionTime;
    this.testId = testId;
  }

  public CompletableFuture<Void> getInitCompleted() {
    return initCompleted;
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:JavaNCSS"})
  public Long start(int ackIntervalMs, int ackBatchSize) {
    boolean useAsync = false;
    LOGGER.info("Starting JMS Consumer");

    long msgCount = 0;
    Acker acker = new Acker(ackBatchSize, ackIntervalMs);
    try (JMSContext jmsContext = jmsFactory.createContext()) {

      Topic jmsTopic = jmsContext.createTopic(this.topic);
      JMSConsumer jmsConsumer = jmsContext.createSharedDurableConsumer(jmsTopic, queue);
      LOGGER.info("JMS Consumer consuming from queue: {}", queue);
      LOGGER.info("JMS Consumer draining old queue data");
      acker.start();
      Message lastMessage = null;
      while (true) {
        Message message = jmsConsumer.receive(100);
        if (message != null) {
          int ackedCount = acker.maybeAck(message);
          msgCount += ackedCount;
          MSG_CONSUMED_COUNT.inc(ackedCount);
        } else {
          if (lastMessage != null) {
            lastMessage.acknowledge();
          }
          break;
        }
        lastMessage = message;
      }

      LOGGER.info("JMS Consumer drained {} old messages", msgCount);

      initCompleted.complete(null);
      acker.stop();

      SampleListener sampleListener = new SampleListener(testId, ackIntervalMs, ackBatchSize);

      if (useAsync) {
        jmsConsumer.setMessageListener(sampleListener);
        while (run) {
          Thread.sleep(1000);
        }
      } else {
        while (run) {
          Message message = jmsConsumer.receive(100);
          sampleListener.onMessage(message);
        }
      }
      if (sampleListener.lastMessage != null) {
        try {
          sampleListener.lastMessage.acknowledge();
        } catch (Exception e) {
          //swallow
        }
      }
      msgCount = sampleListener.msgCount;

    } catch (Exception e) {
      LOGGER.error("JMS Consumer has encountered problems and is shutting down", e);
      throw new RuntimeException(e);
    }
    LOGGER.info("JMS Consumer has completed.");
    return msgCount;
  }

  public void stop() {
    this.run = false;
  }

  protected static boolean isMessageFromCurrentTest(String testId, Message message)
      throws JMSException {

    String msgTestId = message.getStringProperty("test_id");
    return testId.equals(msgTestId);
  }

  protected static void recordSample(Message message) {

    long consumeTs = System.currentTimeMillis();
    try {
      BytesMessage byteMessage = (BytesMessage) message;

      long produceTs = message.getLongProperty("test_msg_ms");

      MESSAGE_LATENCY_HIST.observe(consumeTs - produceTs);

      MSG_CONSUMED_SIZE_GAUGE.set(byteMessage.getBodyLength());
    } catch (Exception e) {
      LOGGER.error("Failed to parse message details.", e);
      MSG_CONSUMED_ERROR_COUNT.inc();
    }
  }

  public static class SampleListener implements MessageListener {

    private final Acker acker;
    private final String testId;
    private int msgCount;
    private Message lastMessage = null;

    private SampleListener(String testdId, int ackIntervalMs, int ackBatchSize) {

      this.acker = new Acker(ackBatchSize, ackIntervalMs);
      this.acker.start();
      this.testId = testdId;
    }

    @Override
    public void onMessage(Message message) {
      try {
        if (message == null) {
          return;
        }

        lastMessage = message;
        int ackCount = acker.maybeAck(message);
        if (ackCount > 0) {
          MSG_CONSUMED_COUNT.inc(ackCount);
          msgCount += ackCount;
          if (isMessageFromCurrentTest(testId, message)) {
            recordSample(message);
          } else {
            LOGGER.error("JMS Consumer received message from another test.");
          }
        }
      } catch (Exception e) {
        LOGGER.error("JMS Consumer, onMessage Listener encountered error, continuing", e);
      }
    }

    public Message getLastMessage() {
      return lastMessage;
    }
  }

  public static class Acker {

    final int ackBatchSize;
    final Duration ackIntervalDuration;
    long logCount;
    int batchCount;
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    Stopwatch logwatch = Stopwatch.createUnstarted();

    public Acker(int ackBatchSize, int ackIntervalMs) {
      this.ackBatchSize = ackBatchSize;
      this.ackIntervalDuration = Duration.ofMillis(ackIntervalMs);
    }

    public Acker start() {
      batchCount = 0;
      logCount = 0;
      logwatch.start();
      stopwatch.start();
      return this;
    }

    public Acker stop() {
      batchCount = 0;
      logCount = 0;
      logwatch.reset();
      stopwatch.reset();
      return this;
    }

    public int maybeAck(Message message) throws JMSException {
      if (message == null) {
        return 0;
      }

      batchCount++;
      logCount++;
      if (ackIntervalDuration.minus(stopwatch.elapsed()).isNegative()
          || ackBatchSize <= batchCount) {

        message.acknowledge();
        if (Duration.ofSeconds(5).minus(logwatch.elapsed()).isNegative()) {
          LOGGER.info("JMS Consumer Acked {} messages in {} ms", logCount,
              logwatch.elapsed().toMillis());
          logCount = 0;
          logwatch.reset().start();
        }
        int ackedCount = batchCount;
        batchCount = 0;
        stopwatch.reset().start();
        return ackedCount;
      }
      return 0;
    }
  }
}
