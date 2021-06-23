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
import io.confluent.mq.perf.TestTime;
import io.confluent.mq.perf.clients.ClientThroughputSync;
import io.confluent.mq.perf.clients.JmsFactory;
import io.confluent.mq.perf.clients.JmsFactory.Connection;
import io.confluent.mq.perf.clients.JmsTestConsumer;
import io.confluent.mq.perf.clients.JmsTestProducer;
import io.confluent.mq.perf.data.DataGenerator;

import java.time.Duration;
import java.util.concurrent.ForkJoinPool;

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

    JmsFactory jmsFactory = new JmsFactory();
    try (Connection connection = jmsFactory.openConnection(parent.getJmsUrl())) {

      DataGenerator dataGenerator = new DataGenerator(parent.getMessageSize());
      ClientThroughputSync sync = new ClientThroughputSync(2);

      Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
      LOGGER.info("Starting test, execution time: {}", executionTime);

      JmsTestProducer jmsProducer = new JmsTestProducer(
          connection,
          "jms2jms-producer",
          jmsTopic,
          dataGenerator,
          executionTime,
          parent.getMessageRate(),
          ttime,
          sync);
      sync.signalReady();

      JmsTestConsumer jmsConsumer = new JmsTestConsumer(
          connection,
          "jms2jms-consumer",
          jmsTopic,
          executionTime,
          ttime,
          sync
      );

      ForkJoinPool.commonPool().execute(jmsConsumer::start);
      LOGGER.info("Waiting for consumer to become ready");
      try {
        sync.awaitReady(Duration.ofSeconds(30));
      } catch (Exception e) {
        throw new RuntimeException("Consumer failed to signal it's ready within the timeout.", e);
      }

      LOGGER.info("Consumer ready, starting producer");
      ForkJoinPool.commonPool().execute(jmsProducer::start);

      LOGGER.info("Awaiting completion");
      boolean completed = false;
      try {
        completed = sync.awaitComplete(executionTime.plus(Duration.ofSeconds(drainTimeout)));
      } catch (Exception e) {
        LOGGER.warn("Error encountered while waiting for test completion.", e);
      }

      if (completed) {
        LOGGER.info("Completed");
      } else {
        LOGGER.warn("Completed without all clients finishing.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to establish connection to JMS broker", e);
    }

  }
}
