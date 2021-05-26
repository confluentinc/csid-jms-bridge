/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.scenarios;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import io.confluent.mq.perf.JmsBridgeParentCommand;
import io.confluent.mq.perf.TestTime;
import io.confluent.mq.perf.clients.JmsTestConsumer;
import io.confluent.mq.perf.clients.JmsTestProducer;
import io.confluent.mq.perf.clients.Starter;
import io.confluent.mq.perf.data.DataGenerator;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Command(name = "jms2jms")
public class Jms2Jms implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Jms2Jms.class);

  @ParentCommand
  private JmsBridgeParentCommand parent;

  @Option(
      names = {"--topic"},
      description = "The name of the JMS topic to publish the sample data to",
      required = true)
  private String jmsTopic;

  @Option(
      names = {"--drain-timeout"},
      description = "The number of seconds to wait for the consumer to drain the queues",
      defaultValue = "30")
  private int drainTimeout = 30;


  @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
  @Override
  public void run() {
    TestTime ttime = new TestTime();

    DataGenerator dataGenerator = new DataGenerator(parent.getMessageSize());

    Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
    LOGGER.info("Starting test, execution time: {}", executionTime);
    JmsTestProducer jmsProducer = new JmsTestProducer(
        parent.getJmsUrl(),
        "jms2jms-producer",
        jmsTopic,
        dataGenerator,
        executionTime,
        parent.getMessageRate(),
        ttime);

    JmsTestConsumer jmsConsumer = new JmsTestConsumer(
        parent.getJmsUrl(),
        "jms2jms-consumer",
        jmsTopic,
        executionTime,
        ttime
    );

    Starter starter = jmsConsumer.createStarter();
    CompletableFuture<?> consumerTask = CompletableFuture.runAsync(starter);
    LOGGER.info("Waiting for consumer to become ready");
    starter.getReadySignal().join();

    LOGGER.info("Consumer ready, starting producer");
    CompletableFuture<?> producerTask = CompletableFuture.runAsync(jmsProducer::start);
    producerTask.join();

    Stopwatch drainTimer = Stopwatch.createStarted();
    Duration timeout = Duration.ofSeconds(drainTimeout);
    long messagesPublishedCount = jmsProducer.getMessagesPublishedCount();

    while (messagesPublishedCount > jmsConsumer.getMessagesConsumedCount()) {

      LOGGER.info("Waiting for the Consumer to drain. Remaining: {}",
          messagesPublishedCount - jmsConsumer.getMessagesConsumedCount());

      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (timeout.minus(drainTimer.elapsed()).isNegative()) {
        LOGGER.info("Stopping consumer even with messages left in the queue due to drain timeout.");
        break;
      }
    }
    LOGGER.info("Stopping the Consumer");
    jmsConsumer.stop();
    consumerTask.join();

    LOGGER.info("Complete");
  }
}
