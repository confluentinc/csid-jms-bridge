/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import io.confluent.mq.perf.scenarios.Jms2Jms;
import io.confluent.mq.perf.scenarios.Jms2Kafka;

@Command(
    name = "jms-bridge-perf",
    mixinStandardHelpOptions = true,
    version = "1.0",
    subcommands = {
        Jms2Kafka.class,
        Jms2Jms.class
    })
public class JmsBridgeParentCommand implements Runnable {
  @Option(
      names = "--pr-port",
      defaultValue = "8888",
      description = "The port that the prometheus metrics should be exposed on.")
  int prometheusPort = 8888;

  @Option(
      names = "--pr-wait",
      defaultValue = "10",
      description =
          "The number of seconds the prometheus server will "
              + "remain available after the test is complete.")
  int prometheusPostWait = 10;


  @Option(
      names = "--delay",
      defaultValue = "0",
      description = "The number of seconds to wait before beginning the test.")
  int testDelay = 0;

  @Option(
      names = {"-sz", "--message-size"},
      defaultValue = "100",
      description = "The size of the payload to be used for the test in bytes")
  int messageSize = 100;

  @Option(
      names = {"-mr", "--message-rate"},
      defaultValue = "1",
      description = "The number of messages to publish per second.")
  int messageRate = 1;

  @Option(
      names = {"-t", "--test-duration"},
      defaultValue = "60",
      description = "The number of seconds the test should be run for")
  int testDuration = 60;

  @Option(
      names = {"--test-id"},
      description = "An ID used to isolate this test's data found in the queue.",
      required = true)
   String testId;

  @Option(
      names = {"--jms-url"},
      defaultValue = "tcp://localhost:61616",
      description = "The connection URL for the JMS broker")
  String jmsUrl = "tcp://localhost:61616";

  @Option(
      names = {"--use-amqp"},
      description = "Use the QPid AMQP JMS client instead of artemis core's",
      defaultValue = "false")
  boolean useAmqp;

  @Override
  public void run() {

  }

  public boolean isUseAmqp() {
    return useAmqp;
  }

  public int getPrometheusPort() {
    return prometheusPort;
  }

  public int getPrometheusPostWait() {
    return prometheusPostWait;
  }

  public int getTestDelay() {
    return testDelay;
  }

  public int getMessageSize() {
    return messageSize;
  }

  public int getMessageRate() {
    return messageRate;
  }

  public int getTestDuration() {
    return testDuration;
  }

  public String getJmsUrl() {
    return jmsUrl;
  }

  public String getTestId() {
    return testId;
  }
}
