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

  @Override
  public void run() {

    JmsFactory jmsFactory = new JmsFactory();
    try (Connection connection = jmsFactory.openConnection(parent.getJmsUrl())) {

      Properties kprops = new Properties();
      try (InputStream propStream = Files.newInputStream(kafkaPropsFile.toPath())) {
        kprops.load(propStream);
      } catch (IOException e) {
        System.err.println("Unable to load kafka properties file");
        e.printStackTrace(System.err);
        throw new RuntimeException(e);
      }

      TestTime ttime = new TestTime();
      DataGenerator dataGenerator = new DataGenerator(parent.getMessageSize());
      ClientThroughputSync sync = new ClientThroughputSync(2);

      final Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
      LOGGER.info("Starting test, execution time: {}", executionTime);
      connection.start();

      final JmsTestProducer jmsProducer = new JmsTestProducer(
          connection,
          "jms2kafka",
          jmsTopic,
          dataGenerator,
          executionTime,
          parent.getMessageRate(),
          ttime,
          sync);
      sync.signalReady();

      KafkaTestConsumer kafkaConsumer = new KafkaTestConsumer(
          kprops,
          kafkaTopic,
          Duration.ofMillis(kafkaPollDurationMs),
          ttime,
          sync);

      CompletableFuture.runAsync(kafkaConsumer::start);
      LOGGER.info("Waiting for consumer to become ready");
      try {
        sync.awaitReady(Duration.ofSeconds(30));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      LOGGER.info("Consumer ready, starting producer");
      CompletableFuture.runAsync(jmsProducer::start);

      LOGGER.info("Awaiting completion");
      boolean completed = false;
      try {
        completed = sync.awaitComplete(executionTime.plus(Duration.ofSeconds(consumerStopDelay)));
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
