/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.server.kafka.KNodeManager;
import java.io.File;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class ConfluentAmqServer extends ActiveMQServerImpl {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(ConfluentAmqServer.class));

  private final KafkaIntegration kafkaIntegration;

  public ConfluentAmqServer() {
    throw new IllegalStateException("Configuration required.");
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration) {
    super(configuration);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQServer parentServer) {
    super(configuration, parentServer);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer) {
    super(configuration, mbeanServer);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, securityManager);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, mbeanServer, securityManager);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    super(configuration, mbeanServer, securityManager, parentServer);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
      final ServiceRegistry serviceRegistry) {
    super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);
    kafkaIntegration = new KafkaIntegration(configuration);
  }

  @Override
  public void checkJournalDirectory() {
    super.checkJournalDirectory();
    try {
      initKafkaIntegration();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private JmsBridgeConfiguration getJmsBridgeConfiguration() {
    return (JmsBridgeConfiguration) getConfiguration();
  }

  private void initKafkaIntegration() throws Exception {
    this.kafkaIntegration.startKafkaIo();
    this.kafkaIntegration.createJournalTopics();
  }

  @Override
  protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
    NodeManager manager;

    final JmsBridgeConfiguration configuration = getJmsBridgeConfiguration();
    final HAPolicyConfiguration.TYPE haType =
        configuration.getHAPolicyConfiguration() == null
            ? null
            : configuration.getHAPolicyConfiguration().getType();

    if (haType == HAPolicyConfiguration.TYPE.SHARED_STORE_MASTER
        || haType == HAPolicyConfiguration.TYPE.SHARED_STORE_SLAVE) {

      if (replicatingBackup) {
        SLOG.error(b -> b
            .event("CreateNodeManager")
            .markFailure()
            .message("replicating backup is not necessary while using kafka persistence"));
        throw new IllegalArgumentException(
            "replicating backup is not necessary while using kafka persistence");
      }
      manager = createKNodeManager(configuration, replicatingBackup, directory);

    } else if (haType == null || haType == HAPolicyConfiguration.TYPE.LIVE_ONLY) {

      SLOG.debug(b -> b
          .event("CreateNodeManager")
          .message("Detected no Shared Store HA options on Kafka store"));

      //LIVE_ONLY should be the default HA option when HA isn't configured
      manager = createKNodeManager(configuration, replicatingBackup, directory);

    } else {
      SLOG.error(b -> b
          .event("CreateNodeManager")
          .markFailure()
          .message("Kafka persistence allows only Shared Store HA options"));
      throw new IllegalArgumentException("Kafka persistence allows only Shared Store HA options");
    }

    return manager;
  }

  private NodeManager createKNodeManager(
      JmsBridgeConfiguration jmsBridgeConfiguration, boolean replicatedBackup, File dir) {

    return new KNodeManager(jmsBridgeConfiguration, kafkaIntegration, replicatedBackup, dir);
  }

  @Override
  protected StorageManager createStorageManager() {
    JmsBridgeConfiguration jmsBridgeConfiguration = getJmsBridgeConfiguration();
    final KafkaJournalStorageManager journal = new KafkaJournalStorageManager(
        kafkaIntegration,
        jmsBridgeConfiguration,
        getCriticalAnalyzer(),
        executorFactory,
        scheduledPool,
        ioExecutorFactory,
        shutdownOnCriticalIO);

    this.getCriticalAnalyzer().add(journal);
    return journal;
  }

  @Override
  public void resetNodeManager() throws Exception {
    SLOG.info(b -> b.event("ResetNodeManager"));
    NodeManager nodeManager = getNodeManager();
    if (nodeManager instanceof KNodeManager) {
      ((KNodeManager) nodeManager).reset();
    } else {
      super.resetNodeManager();
    }
  }

  @Override
  public void stop(boolean failoverOnServerShutdown, boolean criticalIOError, boolean restarting) {
    super.stop(failoverOnServerShutdown, criticalIOError, restarting);
    SLOG.info(b -> b.event("Stop"));
  }
}
