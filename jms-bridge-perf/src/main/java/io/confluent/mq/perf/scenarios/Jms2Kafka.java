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
import io.confluent.mq.perf.clients.JmsTestProducer;
import io.confluent.mq.perf.clients.KafkaTestConsumer;
import io.confluent.mq.perf.clients.Starter;
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

    final Duration executionTime = Duration.ofSeconds(parent.getTestDuration());
    LOGGER.info("Starting test, execution time: {}", executionTime);
    final JmsTestProducer jmsProducer = new JmsTestProducer(
        parent.getJmsUrl(),
        "jms2kafka",
        jmsTopic,
        dataGenerator,
        executionTime,
        parent.getMessageRate(),
        ttime);

    KafkaTestConsumer kafkaConsumer = new KafkaTestConsumer(
        kprops,
        kafkaTopic,
        Duration.ofMillis(kafkaPollDurationMs),
        Duration.ofSeconds(consumerStopDelay),
        executionTime,
        ttime);

    Starter starter = kafkaConsumer.createStarter();
    final CompletableFuture<?> kafkaTask = CompletableFuture.runAsync(starter);
    LOGGER.info("Waiting for consumer to become ready");
    starter.getReadySignal().join();

    LOGGER.info("Consumer ready, starting producer");
    CompletableFuture<?> jmsTask = CompletableFuture.runAsync(jmsProducer::start);
    CompletableFuture.allOf(jmsTask, kafkaTask).join();
    LOGGER.info("Complete");
  }
}
