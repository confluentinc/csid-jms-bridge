/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static com.google.common.io.Resources.getResource;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.io.Resources;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.ConfluentEmbeddedAmq;
import io.confluent.amq.ConfluentEmbeddedAmqImpl;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import io.confluent.amq.test.ServerSpec.Builder;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ArtemisTestServer implements
    BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, Closeable {

  private static final AtomicInteger TEST_SEQ = new AtomicInteger(0);
  private static final StructuredLogger LOGGER = StructuredLogger.with(b -> b
      .loggerClass(ArtemisTestServer.class));

  private Consumer<ServerSpec.Builder> serverSpecBuilder;
  private Consumer<JmsCnxnSpec.Builder> cnxnSpecBuilder;
  private final AtomicInteger nameSeq;
  private final int testSeq;
  private File tempDir;
  private ServerSpec serverSpec;
  private JmsCnxnSpec cnxnSpec;

  private ConfluentEmbeddedAmq embeddedAmq;
  private Connection amqConnection;

  public static Factory factory() {
    return new Factory(TEST_SEQ.getAndIncrement());
  }

  public static ArtemisTestServer embedded(Consumer<Builder> serverSpecBuilder) {

    return embedded(null, serverSpecBuilder);
  }

  public static ArtemisTestServer embedded(
      KafkaTestContainer kafkaTestContainer, Consumer<Builder> serverSpecBuilder) {

    return new Factory(TEST_SEQ.getAndIncrement()).embedded(kafkaTestContainer, serverSpecBuilder);
  }

  protected ArtemisTestServer(
      int testSeq,
      AtomicInteger nameSeq,
      Consumer<Builder> serverSpecBuilder,
      Consumer<JmsCnxnSpec.Builder> cnxnSpecBuilder) {

    this.testSeq = testSeq;
    this.nameSeq = nameSeq;
    this.serverSpecBuilder = serverSpecBuilder;
    this.cnxnSpecBuilder = cnxnSpecBuilder;
  }

  public String bridgeId() {
    return this.serverSpec.jmsBridgeConfig().id();
  }

  public ConfluentAmqServer confluentAmqServer() {
    return this.embeddedAmq.getConfluentAmq();
  }

  public ActiveMQServerControl serverControl() {
    return this.embeddedAmq.getAmq().getActiveMQServerControl();
  }

  public String safeId(String prefix) {
    return String.format("%s-%d-%d", prefix, testSeq, nameSeq.getAndIncrement());
  }

  public String messageJournalTopic() {
    return KafkaJournal.journalTopic(
        serverSpec.jmsBridgeConfig().id(),
        KafkaJournalStorageManager.MESSAGES_NAME);
  }

  public String bindingsJournalTopic() {
    return KafkaJournal.journalTopic(
        serverSpec.jmsBridgeConfig().id(),
        KafkaJournalStorageManager.BINDINGS_NAME);
  }

  public void assertAddressAvailable(String address) {
    TestSupport.retry(10, 500, () ->
        assertTrue(
            Arrays.asList(serverControl().getAddressNames()).contains(address)));
  }

  public String consumerGroupName() {
    return KafkaIntegration.applicationId(serverSpec.jmsBridgeConfig().id());
  }

  public ArtemisTestServer start() throws Exception {
    beforeAll();
    beforeEach();
    return this;
  }

  @Override
  public void close() {
    stop();
  }

  public void stop() {
    try {
      afterEach();
      afterAll();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    beforeAll();
  }

  private void beforeAll() throws Exception {

    if (this.serverSpecBuilder != null) {
      this.cnxnSpec = new JmsCnxnSpec.Builder().build();

      ServerSpec.Builder specBldr = new ServerSpec.Builder();
      this.serverSpecBuilder.accept(specBldr);

      if (specBldr.dataDirectory().isPresent()) {
        this.tempDir = new File(specBldr.dataDirectory().get());
      } else {
        this.tempDir = Files.createTempDirectory("junit-jms-bridge").toFile();
      }

      Path streamDir = this.tempDir.toPath().resolve(safeId("streams-state-dir"));
      Files.createDirectory(streamDir);
      specBldr.mutateJmsBridgeConfig(b -> b
          .putStreams(StreamsConfig.STATE_DIR_CONFIG, streamDir.toAbsolutePath().toString())
      );
      this.serverSpec = specBldr.build();

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

  private void beforeEach() throws Exception {
    getConnection();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    afterEach();
  }

  private void afterEach() throws Exception {
    disposeConnection();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    afterAll();
  }

  private void afterAll() throws Exception {
    if (this.embeddedAmq != null) {
      this.embeddedAmq.stop();
    }

    if (!this.serverSpec.dataDirectory().isPresent()
        && this.tempDir != null) {

      MoreFiles.deleteRecursively(this.tempDir.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
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
        this.amqConnection.close();
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
              maybeCreateDirectory(amqDataDir.resolve("bindings")).toAbsolutePath().toString());
          config.setJournalDirectory(
              maybeCreateDirectory(amqDataDir.resolve("journal")).toAbsolutePath().toString());
          config.setPagingDirectory(
              maybeCreateDirectory(amqDataDir.resolve("Paging")).toAbsolutePath().toString());
          config.setLargeMessagesDirectory(
              maybeCreateDirectory(amqDataDir.resolve("large-messages")).toAbsolutePath()
                  .toString());
          config.setNodeManagerLockDirectory(
              maybeCreateDirectory(amqDataDir.resolve("node-manager-lock")).toAbsolutePath()
                  .toString());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      BridgeConfig bridgeConfig = BridgeConfig.Builder.from(spec.jmsBridgeConfig())
          .putKafka("group.id", spec.groupId())
          .build();

      return new JmsBridgeConfiguration(config, bridgeConfig);
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

  private static Path maybeCreateDirectory(Path dirPath) {
    if (!Files.exists(dirPath)) {
      try {
        return Files.createDirectory(dirPath);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      return dirPath;
    }
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

  public static class Factory {

    private final int testNumber;
    private final AtomicInteger nameSeq = new AtomicInteger(0);
    private KafkaTestContainer prepedKafkaTestContainer;
    private Consumer<Builder> preppedServerSpecBuilder;

    public Factory(int testNumber) {
      this.testNumber = testNumber;
    }

    public String safeId(String prefix) {
      return String.format("%s-%d-%d", prefix, testNumber, nameSeq.getAndIncrement());
    }

    public void prepare(
        KafkaTestContainer kafkaTestContainer, Consumer<Builder> serverSpecBuilder) {
      this.prepedKafkaTestContainer = kafkaTestContainer;
      this.preppedServerSpecBuilder = serverSpecBuilder;
    }

    public ArtemisTestServer start() throws Exception {
      ArtemisTestServer testServer = embedded(prepedKafkaTestContainer, preppedServerSpecBuilder);
      return testServer.start();
    }

    public ArtemisTestServer embedded(
        KafkaTestContainer kafkaTestContainer, Consumer<Builder> serverSpecBuilder) {

      Function<String, String> makeId = prefix -> prefix + "-" + testNumber;

      //overridable defaults
      BridgeConfig.Builder defaultBridgeConfig = BridgeConfigFactory.loadConfiguration(
          Resources.getResource("base-test-config.conf"));

      Consumer<ServerSpec.Builder> specConsumer = b -> b
          .brokerXml(getResource("broker.xml").toString())
          .mutateJmsBridgeConfig(br -> br.mergeFrom(defaultBridgeConfig));

      //add their configuration
      specConsumer = specConsumer.andThen(serverSpecBuilder);

      //after their configuration we override
      specConsumer = specConsumer.andThen(b -> b
          .mutateJmsBridgeConfig(jb -> jb
              .id(makeId.apply("test-bridge")))
          .groupId(makeId.apply("junit")));

      if (kafkaTestContainer != null) {
        specConsumer = specConsumer.andThen(b -> b
            .mutateJmsBridgeConfig(jb -> jb
                .putAllKafka(BridgeConfigFactory.propsToMap(kafkaTestContainer.defaultProps()))
                .putAllStreams(BridgeConfigFactory.propsToMap(kafkaTestContainer.defaultProps()))));
      }

      return new ArtemisTestServer(
          testNumber, nameSeq, specConsumer, null);

    }
  }
}
