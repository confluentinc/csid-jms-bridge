/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.scenarios;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import io.confluent.mq.perf.JmsBridgeParentCommand;
import io.confluent.mq.perf.clients.JmsFactory;
import io.confluent.mq.perf.clients.JmsTestConsumer;
import io.confluent.mq.perf.clients.JmsTestProducer;
import io.confluent.mq.perf.data.DataGenerator;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
      names = {"--queue"},
      description = "The name of the queue to consume from.",
      required = true)
  private String queue;

  @Option(
      names = {"--ack-interval-ms"},
      description = "The number of milliseconds to wait before acking",
      defaultValue = "5000")
  private int ackIntervalMs = 5000;

  @Option(
      names = {"--ack-batch-sz"},
      description = "The number of records to receive before acking",
      defaultValue = "100")
  private int ackBatchSize = 100;

  @Option(
      names = {"--drain-timeout"},
      description = "The number of seconds to wait for the consumer to drain the queues",
      defaultValue = "120")
  private int drainTimeout = 120;

  @Option(
      names = {"--activity"},
      description =
          "Indicate the scenario activiy to perform, ${COMPLETION-CANDIDATES}, "
              + "defaults to ${DEFAULT-VALUE}.",
      type = ActivityOptions.class,
      defaultValue = "pubsub")
  private ActivityOptions activity;

  @Option(
      names = {"--pub-async"},
      description = "Whether to publishly messages asynchronously or not.",
      defaultValue = "false")
  private boolean pubAsync;

  @SuppressWarnings({
      "checkstyle:VariableDeclarationUsageDistance",
      "checkstyle:CyclomaticComplexity",
      "checkstyle:NPathComplexity"
  })
  @Override
  public void run() {
    JmsFactory jmsFactory = new JmsFactory(parent.getJmsUrl(), parent.isUseAmqp());

    DataGenerator dataGenerator = new DataGenerator(parent.getMessageSize());

    Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
    LOGGER.info("Starting test, execution time: {}", executionTime);

    JmsTestProducer jmsProducer = new JmsTestProducer(
        jmsFactory,
        jmsTopic,
        dataGenerator,
        executionTime,
        parent.getMessageRate(),
        pubAsync,
        parent.getTestId()
    );

    JmsTestConsumer jmsConsumer = new JmsTestConsumer(
        jmsFactory,
        jmsTopic,
        queue,
        executionTime,
        parent.getTestId()
    );

    CompletableFuture<Long> consumerFuture = CompletableFuture.completedFuture(0L);
    if (activity.subActive()) {

      consumerFuture = CompletableFuture.supplyAsync(
          () -> jmsConsumer.start(ackIntervalMs, ackBatchSize));
      LOGGER.info("Waiting for consumer to become ready");
      try {
        jmsConsumer.getInitCompleted().get(drainTimeout, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException("Consumer failed to signal it's ready within the timeout.", e);
      }

      LOGGER.info("Consumer ready.");
    }

    if (activity.pubActive()) {
      LOGGER.info("Producer starting.");
      CompletableFuture<Long> producerFuture = CompletableFuture.supplyAsync(jmsProducer::start);

      LOGGER.info("Waiting for producer to finish");

      try {
        Long publishCount = producerFuture.get();
        LOGGER.info("JMS Publisher finished, published count: {}", publishCount);
      } catch (ExecutionException | InterruptedException e) {
        LOGGER.warn("Execution interrupted.");
      }
    }

    try {
      if (activity.subActive()) {
        LOGGER.info("Draining consumer in preparation for completion");
        Thread.sleep(drainTimeout * 1000);
        jmsConsumer.stop();
        Long consumedCount = consumerFuture.get();
        LOGGER.info("Consumer completed, consumed count: {}", consumedCount);
      }

    } catch (Exception e) {
      LOGGER.warn("Error encountered while waiting for test completion.", e);
      throw new RuntimeException(e);
    }

    LOGGER.info("Test completed");
  }

  public enum ActivityOptions {
    pub, sub, pubsub;

    public boolean pubActive() {
      return this == pub || this == pubsub;
    }

    public boolean subActive() {
      return this == sub || this == pubsub;
    }
  }
}
