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
import io.confluent.mq.perf.clients.JmsTestProducer;
import io.confluent.mq.perf.clients.KafkaTestConsumer;
import io.confluent.mq.perf.data.DataGenerator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Command(name = "jms2kafka")
public class Jms2Kafka implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Jms2Kafka.class);

  @ParentCommand
  private JmsBridgeParentCommand parent;

  @Option(
      names = {"-kpf", "--kafka-props-file"},
      description = "Path to a file containing the kafka consumer client configuration",
      required = true)
  private File kafkaPropsFile;

  @Option(
      names = {"-jtp", "--jms-topic"},
      description = "The name of the JMS topic to publish the sample data to",
      required = true)
  private String jmsTopic;

  @Option(
      names = {"-ktp", "--kafka-topic"},
      description = "The name of the Kafka topic to consume the sample data from",
      required = true)
  private String kafkaTopic;

  @Option(
      names = {"--kafka-poll-duration-ms"},
      defaultValue = "100",
      description = "The number of milliseconds the Kafka consumer will poll before returning.")
  private int kafkaPollDurationMs = 100;

  @Option(
      names = "--consumer-stop-delay",
      defaultValue = "10",
      description = "The number of seconds to delay the stop of the kafka consumer")
  private int consumerStopDelay = 10;

  @Option(
      names = {"--jms-async"},
      description = "Whether to publish JMS messages asynchronously or not.",
      defaultValue = "false")
  private boolean jmsAsync;

  @Option(
      names = {"--activity"},
      description =
          "Indicate the scenario activiy to perform, ${COMPLETION-CANDIDATES}, "
              + "defaults to ${DEFAULT-VALUE}.",
      type = ActivityOptions.class,
      defaultValue = "pubsub")
  private ActivityOptions activity;


  @Override
  public void run() {

    JmsFactory jmsFactory = new JmsFactory(parent.getJmsUrl(), parent.isUseAmqp());

    Properties kprops = new Properties();
    try (InputStream propStream = Files.newInputStream(kafkaPropsFile.toPath())) {
      kprops.load(propStream);
    } catch (IOException e) {
      System.err.println("Unable to load kafka properties file");
      e.printStackTrace(System.err);
      throw new RuntimeException(e);
    }

    final Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
    LOGGER.info("Starting test, execution time: {}", executionTime);

    CompletableFuture<Long> consumerFuture = CompletableFuture.completedFuture(0L);
    KafkaTestConsumer kafkaConsumer = null;
    if (activity.subActive()) {
      kafkaConsumer = new KafkaTestConsumer(
          kprops,
          kafkaTopic,
          Duration.ofMillis(kafkaPollDurationMs),
          parent.getTestId());

      consumerFuture = CompletableFuture.supplyAsync(kafkaConsumer::start);
      LOGGER.info("Waiting for consumer to become ready");
      try {
        kafkaConsumer.getInitCompleted().get(30, TimeUnit.SECONDS);
        LOGGER.info("Consumer ready");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (activity.pubActive()) {
      DataGenerator dataGenerator = new DataGenerator(parent.getMessageSize());
      final JmsTestProducer jmsProducer = new JmsTestProducer(
          jmsFactory,
          jmsTopic,
          dataGenerator,
          executionTime,
          parent.getMessageRate(),
          jmsAsync,
          parent.getTestId());

      LOGGER.info("Starting producer");
      CompletableFuture<Long> producerFuture = CompletableFuture.supplyAsync(jmsProducer::start);

      LOGGER.info("Waiting for producer to complete.");
      try {
        Long producedCount = producerFuture.get();
        LOGGER.info("Producer complete, produced {} records.", producedCount);

      } catch (Exception e) {
        LOGGER.warn("Error encountered while waiting for producer to complete.", e);
      }
    }

    if (activity.subActive()) {
      LOGGER.info("Giving");
      kafkaConsumer.stop();
      try {
        Long consumedCount = consumerFuture.get();
        LOGGER.info("Consumer complete, consumed {} records.", consumedCount);

      } catch (Exception e) {
        LOGGER.warn("Error encountered while waiting for consumer to complete.", e);
      }
    }

    LOGGER.info("Test Completed");
  }
}
