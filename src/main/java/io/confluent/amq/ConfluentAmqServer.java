/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class ConfluentAmqServer extends ActiveMQServerImpl {

  public ConfluentAmqServer() {
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration) {
    super(configuration);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration, final ActiveMQServer parentServer) {
    super(configuration, parentServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration, final MBeanServer mbeanServer) {
    super(configuration, mbeanServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, securityManager);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, mbeanServer, securityManager);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    super(configuration, mbeanServer, securityManager, parentServer);
  }

  public ConfluentAmqServer(final JmsBridgeConfiguration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
      final ServiceRegistry serviceRegistry) {
    super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);
  }

  @Override
  protected StorageManager createStorageManager() {
    final JmsBridgeConfiguration configuration = (JmsBridgeConfiguration) getConfiguration();

    final KafkaJournalStorageManager journal = new KafkaJournalStorageManager(
        configuration, getCriticalAnalyzer(), executorFactory, scheduledPool,
        ioExecutorFactory, shutdownOnCriticalIO);

    this.getCriticalAnalyzer().add(journal);
    return journal;
  }


}
