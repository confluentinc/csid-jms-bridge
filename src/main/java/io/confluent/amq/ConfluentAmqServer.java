/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import java.util.Properties;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class ConfluentAmqServer extends ActiveMQServerImpl {

  private Properties kafkaProps;

  public ConfluentAmqServer() {
  }

  public ConfluentAmqServer(final Configuration configuration) {
    super(configuration);
  }

  public ConfluentAmqServer(final Configuration configuration, final ActiveMQServer parentServer) {
    super(configuration, parentServer);
  }

  public ConfluentAmqServer(final Configuration configuration, final MBeanServer mbeanServer) {
    super(configuration, mbeanServer);
  }

  public ConfluentAmqServer(final Configuration configuration,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, securityManager);
  }

  public ConfluentAmqServer(final Configuration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager) {
    super(configuration, mbeanServer, securityManager);
  }

  public ConfluentAmqServer(final Configuration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    super(configuration, mbeanServer, securityManager, parentServer);
  }

  public ConfluentAmqServer(final Configuration configuration, final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
      final ServiceRegistry serviceRegistry) {
    super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);
  }

  public void setKafkaProps(final Properties kafkaProps) {
    this.kafkaProps = kafkaProps;
  }


  @Override
  protected StorageManager createStorageManager() {
    if (this.kafkaProps == null || this.kafkaProps.isEmpty()) {
      throw new IllegalStateException("KafkaProps must be set before starting the server.");
    }

    final Configuration configuration = getConfiguration();

    final KafkaJournalStorageManager journal = new KafkaJournalStorageManager(
        kafkaProps, configuration, getCriticalAnalyzer(), executorFactory, scheduledPool,
        ioExecutorFactory, shutdownOnCriticalIO);

    this.getCriticalAnalyzer().add(journal);
    return journal;
  }


}
