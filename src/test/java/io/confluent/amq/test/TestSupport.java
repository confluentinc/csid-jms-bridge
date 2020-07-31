/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static com.google.common.io.Resources.getResource;

import io.confluent.amq.ConfluentEmbeddedAmq;
import io.confluent.amq.ConfluentEmbeddedAmqImpl;
import io.confluent.amq.test.ServerSpec.Builder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;

public final class TestSupport {

  private TestSupport() {
  }

  @SuppressWarnings("UnstableApiUsage")
  public static ConfluentEmbeddedAmq createEmbeddedAmq(Consumer<ServerSpec.Builder> specMutater) {
    //set some test defaults
    ServerSpec.Builder specBuilder = new Builder()
        .bridgeId("junit")
        .groupId("junit")
        .brokerXml(getResource("broker.xml").toString());

    specMutater.accept(specBuilder);
    ServerSpec spec = specBuilder.build();

    ConfluentEmbeddedAmqImpl amqServer = new ConfluentEmbeddedAmqImpl
        .Builder(spec.brokerXml(), spec.jmsBridgeProps()).build();

    Configuration amqConf = amqServer.getAmq().getConfiguration();

    Path amqDataDir = new File(spec.dataDirectory()).toPath();
    try {
      amqConf.setBindingsDirectory(
          Files.createDirectory(amqDataDir.resolve("bindings")).toAbsolutePath().toString());
      amqConf.setJournalDirectory(
          Files.createDirectory(amqDataDir.resolve("journal")).toAbsolutePath().toString());
      amqConf.setPagingDirectory(
          Files.createDirectory(amqDataDir.resolve("Paging")).toAbsolutePath().toString());
      amqConf.setLargeMessagesDirectory(
          Files.createDirectory(amqDataDir.resolve("large-messages")).toAbsolutePath().toString());
      amqConf.setNodeManagerLockDirectory(
          Files.createDirectory(amqDataDir.resolve("node-manager-lock")).toAbsolutePath()
              .toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

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
    JmsCnxnSpec.Builder specBuilder = new JmsCnxnSpec.Builder()
        .url("tcp://localhost:61616");
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
