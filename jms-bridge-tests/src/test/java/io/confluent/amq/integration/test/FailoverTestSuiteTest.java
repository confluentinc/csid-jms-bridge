/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.JmsBridgeConfiguration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.testcontainers.containers.KafkaContainer;

@RunWith(JmsSuiteRunner.class)
@JmsSuiteRunner.JmsSuitePackages(
    includePackages = {
        "org.apache.activemq.artemis.tests.integration.jms",
        "org.apache.activemq.artemis.tests.integration.cluster.failover"
    },
    includeClasses = {
        FailoverTest.class
    },
    includeTests = {
        "FailoverTest#testTransactedMessagesSentSoRollbackAndContinueWork"
    },
    excludeClasses = {},
    excludePackages = {},
    excludeTests = {}
)
public class FailoverTestSuiteTest {

  private FailoverTestSuiteTest() {
  }

  public static AtomicInteger BRIDGE_ID_SEQUENCE = new AtomicInteger(1);
  public static AtomicInteger NODED_ID_SEQUENCE = new AtomicInteger(1);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static KafkaContainer kafkaContainer =
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  public static JmsBridgeConfiguration wrapConfig(Configuration amqConfig) {
    try {
      Properties kafkaProps = new Properties();
      String bridgeId = "unit-test-" + FailoverTestSuiteTest.BRIDGE_ID_SEQUENCE.get();
      kafkaProps.setProperty(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          FailoverTestSuiteTest.kafkaContainer.getBootstrapServers());
      kafkaProps.setProperty("bridge.id", bridgeId);
      kafkaProps.setProperty(
          StreamsConfig.STATE_DIR_CONFIG,
          FailoverTestSuiteTest.temporaryFolder
              .newFolder(bridgeId + "-" + NODED_ID_SEQUENCE.incrementAndGet()).getAbsolutePath());
      kafkaProps.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "6000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "2000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), "5000");
      kafkaProps.setProperty(
          StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");

      JmsBridgeConfiguration jmsBridgeConfiguration = new JmsBridgeConfiguration(amqConfig,
          kafkaProps);

      return jmsBridgeConfiguration;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

}
