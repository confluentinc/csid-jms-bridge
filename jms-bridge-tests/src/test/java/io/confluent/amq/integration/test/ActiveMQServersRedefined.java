/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.JmsBridgeConfiguration;
import java.util.Properties;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Used for intercepting static method creation of active mq servers in test classes.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class ActiveMQServersRedefined {

  public static ActiveMQServer intercept(
      final Configuration config,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager,
      final boolean enablePersistence) {

    config.setPersistenceEnabled(enablePersistence);

    try {
      Properties kafkaProps = new Properties();
      String bridgeId = "unit-test-" + JmsTestSuiteTest.BRIDGE_ID_SEQUENCE.incrementAndGet();
      kafkaProps.setProperty(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          JmsTestSuiteTest.kafkaContainer.getBootstrapServers());
      kafkaProps.setProperty("bridge.id", bridgeId);
      kafkaProps.setProperty(
          StreamsConfig.STATE_DIR_CONFIG,
          JmsTestSuiteTest.temporaryFolder.newFolder(bridgeId).getAbsolutePath());
      kafkaProps.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "6000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "2000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), "5000");

      JmsBridgeConfiguration jmsBridgeConfiguration = new JmsBridgeConfiguration(config,
          kafkaProps);

      System.out.println("ConfluentAmqServer intercepted ActiveMQServerImpl creation.");

      return new ConfluentAmqServer(
          jmsBridgeConfiguration,
          mbeanServer,
          securityManager);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
