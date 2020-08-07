/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static com.google.common.io.Resources.getResource;

import io.confluent.amq.ConfluentEmbeddedAmq;
import io.confluent.amq.ConfluentEmbeddedAmqImpl;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.test.ServerSpec.Builder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ArtemisTestServer implements
    BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final StructuredLogger LOGGER = StructuredLogger.with(b -> b
      .loggerClass(ArtemisTestServer.class));

  private final Consumer<ServerSpec.Builder> serverSpecBuilder;
  private final Consumer<JmsCnxnSpec.Builder> cnxnSpecBuilder;
  private Path tempDir;
  private ServerSpec serverSpec;
  private JmsCnxnSpec cnxnSpec;

  private ConfluentEmbeddedAmq embeddedAmq;
  private Connection amqConnection;

  public static ArtemisTestServer embedded(Consumer<ServerSpec.Builder> serverSpecBuilder) {

    Consumer<ServerSpec.Builder> specConsumer = b -> b
        .bridgeId("junit")
        .groupId("junit")
        .brokerXml(getResource("broker.xml").toString());

    return new ArtemisTestServer(specConsumer.andThen(serverSpecBuilder), null);
  }

  public static ArtemisTestServer remote(Consumer<JmsCnxnSpec.Builder> cnxnSpecBuilder) {
    Consumer<JmsCnxnSpec.Builder> specConsumer = b -> {
    };

    return new ArtemisTestServer(null, specConsumer.andThen(cnxnSpecBuilder));
  }

  protected ArtemisTestServer(
      Consumer<Builder> serverSpecBuilder,
      Consumer<JmsCnxnSpec.Builder> cnxnSpecBuilder) {

    this.serverSpecBuilder = serverSpecBuilder;
    this.cnxnSpecBuilder = cnxnSpecBuilder;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {

    if (this.serverSpecBuilder != null) {
      this.cnxnSpec = new JmsCnxnSpec.Builder().build();

      ServerSpec.Builder specBldr = new ServerSpec.Builder();
      this.serverSpecBuilder.accept(specBldr);
      this.serverSpec = specBldr.build();

      if (this.serverSpec.dataDirectory().isPresent()) {
        this.tempDir = new File(this.serverSpec.dataDirectory().get()).toPath();
      } else {
        this.tempDir = Files.createTempDirectory("junit-jms-bridge");
      }

      if (this.serverSpec.useVanilla()) {
        embeddedAmq = createVanillaEmbeddedAmq(this.serverSpec);
      } else {
        embeddedAmq = createEmbeddedAmq(this.serverSpec);
      }

      embeddedAmq.start();

    } else {
      JmsCnxnSpec.Builder specBldr = new JmsCnxnSpec.Builder();
      this.cnxnSpecBuilder.accept(specBldr);
      this.cnxnSpec = specBldr.build();

      try (Connection cnxn = createJmsConnection(this.cnxnSpec)) {
        ConnectionMetaData meta = cnxn.getMetaData();
        String jmsProvider = meta.getJMSProviderName();
        String jmsProviderVersion = meta.getProviderVersion();
        String jmsVersion = meta.getJMSVersion();

        LOGGER.info(b -> b
            .event("RemoteJMSConnection")
            .markSuccess()
            .putTokens("JMS-Provider-Name", jmsProvider)
            .putTokens("JMS-Provider-Version", jmsProviderVersion)
            .putTokens("JMS-Version", jmsVersion));
      } catch (Exception e) {
        LOGGER.info(b -> b
            .event("RemoteJMSConnection")
            .markFailure()
            .putTokens("url", this.cnxnSpec.url()), e);

      }
    }
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    getConnection();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    disposeConnection();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (this.embeddedAmq != null) {
      this.embeddedAmq.stop();
    }
  }

  public synchronized void restartServer() throws Exception {
    afterEach(null);
    afterAll(null);
    beforeAll(null);
  }

  public synchronized Connection getConnection() {
    if (this.amqConnection == null) {
      this.amqConnection = startJmsConnection(this.cnxnSpec);
    }

    return this.amqConnection;
  }

  private synchronized void disposeConnection() {
    if (this.amqConnection != null) {
      try {
        this.amqConnection.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      this.amqConnection = null;
    }
  }

  /**
   * Generate a JMS Bridge configuration using the provided specification.
   */
  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  private static JmsBridgeConfiguration configuration(ServerSpec spec) {

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

  /**
   * Creates an unmodified embedded Artemis server (see {@link EmbeddedActiveMQ} ). This can be used
   * to compare the JMS Bridge version with.
   */
  private static ConfluentEmbeddedAmq createVanillaEmbeddedAmq(ServerSpec spec) {

    JmsBridgeConfiguration config = configuration(spec);

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

  /**
   * Create the modified embedded Artemis server that powers the JMS Bridge.
   */
  @SuppressWarnings("UnstableApiUsage")
  public static ConfluentEmbeddedAmq createEmbeddedAmq(ServerSpec spec) {

    JmsBridgeConfiguration config = configuration(spec);
    return new ConfluentEmbeddedAmqImpl.Builder(config).build();
  }

  /**
   * Like {@link #createJmsConnection(JmsCnxnSpec)} but also starts it. Remember to close it when
   * done.
   */
  private Connection startJmsConnection(JmsCnxnSpec spec) {
    Connection conn = createJmsConnection(spec);
    try {
      conn.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return conn;
  }

  /**
   * Create a JMS connection based on the specification provided. Does not start the connection so
   * the caller will need to do that before using it.
   */
  private Connection createJmsConnection(JmsCnxnSpec spec) {
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
