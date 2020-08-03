/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static com.google.common.io.Resources.getResource;

import io.confluent.amq.ConfluentEmbeddedAmq;
import io.confluent.amq.ConfluentEmbeddedAmqImpl;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.test.ServerSpec.Builder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public final class TestSupport {

  private TestSupport() {
  }

  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  public static JmsBridgeConfiguration defaultConfiguration(Consumer<ServerSpec.Builder> mutator) {
    ServerSpec.Builder specBuilder = new Builder()
        .bridgeId("junit")
        .groupId("junit")
        .brokerXml(getResource("broker.xml").toString());

    mutator.accept(specBuilder);
    ServerSpec spec = specBuilder.build();

    try {
      FileDeploymentManager deploymentManager = new FileDeploymentManager(spec.brokerXml());
      FileConfiguration config = new FileConfiguration();
      LegacyJMSConfiguration legacyJMSConfiguration = new LegacyJMSConfiguration(config);
      deploymentManager.addDeployable(config).addDeployable(legacyJMSConfiguration);
      deploymentManager.readConfiguration();

      spec.dataDirectory().ifPresent(dataDir -> {
        Path amqDataDir = new File(dataDir).toPath();
        try {
          config.setBindingsDirectory(
              Files.createDirectory(amqDataDir.resolve("bindings")).toAbsolutePath().toString());
          config.setJournalDirectory(
              Files.createDirectory(amqDataDir.resolve("journal")).toAbsolutePath().toString());
          config.setPagingDirectory(
              Files.createDirectory(amqDataDir.resolve("Paging")).toAbsolutePath().toString());
          config.setLargeMessagesDirectory(
              Files.createDirectory(amqDataDir.resolve("large-messages")).toAbsolutePath()
                  .toString());
          config.setNodeManagerLockDirectory(
              Files.createDirectory(amqDataDir.resolve("node-manager-lock")).toAbsolutePath()
                  .toString());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      return new JmsBridgeConfiguration(config, spec.jmsBridgeProps());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ConfluentEmbeddedAmq createVanillaEmbeddedAmq(
      Consumer<ServerSpec.Builder> specMutater) {

    JmsBridgeConfiguration config = defaultConfiguration(specMutater);

    EmbeddedActiveMQ amqServer = new EmbeddedActiveMQ();
    amqServer.setConfiguration(config);

    return new ConfluentEmbeddedAmq() {
      @Override
      public void start() throws Exception {
        amqServer.start();
      }

      @Override
      public void stop() throws Exception {
        amqServer.stop();
      }
    };
  }

  @SuppressWarnings("UnstableApiUsage")
  public static ConfluentEmbeddedAmq createEmbeddedAmq(Consumer<ServerSpec.Builder> specMutater) {

    JmsBridgeConfiguration config = defaultConfiguration(specMutater);
    ConfluentEmbeddedAmqImpl amqServer = new ConfluentEmbeddedAmqImpl
        .Builder(config).build();

    return amqServer;
  }

  public static Connection startJmsConnection(Consumer<JmsCnxnSpec.Builder> specMutator) {
    Connection conn = createJmsConnection(specMutator);
    try {
      conn.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return conn;
  }

  public static Connection createJmsConnection(Consumer<JmsCnxnSpec.Builder> specMutator) {
    JmsCnxnSpec.Builder specBuilder = new JmsCnxnSpec.Builder();
    specMutator.accept(specBuilder);
    JmsCnxnSpec spec = specBuilder.build();

    try {
      ConnectionFactory cf = ActiveMQJMSClient
          .createConnectionFactory(spec.url(), spec.name().orElse("junit"));
      Connection amqConnection = cf.createConnection();
      amqConnection.setClientID(spec.clientId().orElse("junit"));

      return amqConnection;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
