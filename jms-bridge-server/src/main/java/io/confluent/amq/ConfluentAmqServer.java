/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.server.kafka.KafkaNodeManager;
import java.io.File;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentAmqServer extends ActiveMQServerImpl {

  private volatile KafkaJournalStorageManager kafKaStorageManager;

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfluentAmqServer.class);

  public ConfluentAmqServer() {
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration) {
    super(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQServer parentServer) {
    super(configuration, parentServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer) {
    super(configuration, mbeanServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, securityManager);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, mbeanServer, securityManager);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    super(configuration, mbeanServer, securityManager, parentServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
      final ServiceRegistry serviceRegistry) {
    super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);
  }

  @Override
  protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
    NodeManager manager;
    KafkaJournalStorageManager journal = (KafkaJournalStorageManager) createStorageManager();
    final JmsBridgeConfiguration configuration = (JmsBridgeConfiguration) getConfiguration();

    final HAPolicyConfiguration.TYPE haType =
        configuration.getHAPolicyConfiguration() == null
            ? null
            : configuration.getHAPolicyConfiguration().getType();

    if (haType == HAPolicyConfiguration.TYPE.SHARED_STORE_MASTER
        || haType == HAPolicyConfiguration.TYPE.SHARED_STORE_SLAVE) {

      if (replicatingBackup) {
        throw new IllegalArgumentException(
            "replicatingBackup is not supported yet while using Kafka persistence");
      }
      KafkaNodeManager kafkaManager = new KafkaNodeManager();
      journal.registerListener(kafkaManager);
      manager = kafkaManager;

    } else if (haType == null || haType == HAPolicyConfiguration.TYPE.LIVE_ONLY) {

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Detected no Shared Store HA options on Kafka store");
      }
      //LIVE_ONLY should be the default HA option when HA isn't configured
      manager = new FileLockNodeManager(
          directory,
          replicatingBackup,
          configuration.getJournalLockAcquisitionTimeout(),
          scheduledPool);

    } else {
      throw new IllegalArgumentException("Kafka persistence allows only Shared Store HA options");
    }

    return manager;
  }

  @Override
  protected synchronized StorageManager createStorageManager() {
    if (kafKaStorageManager == null) {
      final JmsBridgeConfiguration configuration = (JmsBridgeConfiguration) getConfiguration();

      final KafkaJournalStorageManager journal = new KafkaJournalStorageManager(
          configuration, getCriticalAnalyzer(), executorFactory, scheduledPool,
          ioExecutorFactory, shutdownOnCriticalIO);

      this.getCriticalAnalyzer().add(journal);
      kafKaStorageManager = journal;
    }
    return kafKaStorageManager;
  }


}
