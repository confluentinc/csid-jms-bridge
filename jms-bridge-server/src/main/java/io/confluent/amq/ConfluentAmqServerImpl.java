/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.cli.AutoKillSwitch;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.exchange.KafkaExchangeManager;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.server.kafka.KNodeManager;
import io.confluent.amq.server.kafka.KafkaNodeManager;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

import javax.management.MBeanServer;
import java.io.File;
import java.time.Duration;

public class ConfluentAmqServerImpl extends ActiveMQServerImpl implements ConfluentAmqServer {
  private static final String DEFAULT_KILL_DURATION_MINUTES = String.valueOf(60 * 8);
  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
          .loggerClass(DelegatingConfluentAmqServer.class));

  private final KafkaIntegration kafkaIntegration;
  private final KafkaExchangeManager kafkaExchangeManager;

  private final AutoKillSwitch killSwitch;

  public ConfluentAmqServerImpl() {
    throw new IllegalStateException("Configuration required.");
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration) {
    this(configuration, null, null);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final ActiveMQServer parentServer) {
    this(configuration, null, null, parentServer);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final MBeanServer mbeanServer) {
    this(configuration, mbeanServer, null);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final ActiveMQSecurityManager securityManager) {
    this(configuration, null, securityManager);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final MBeanServer mbeanServer,
                                final ActiveMQSecurityManager securityManager) {
    this(configuration, mbeanServer, securityManager, null);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final MBeanServer mbeanServer,
                                final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    this(configuration, mbeanServer, securityManager, parentServer, null);
  }

  public ConfluentAmqServerImpl(final JmsBridgeConfiguration configuration,
                                final MBeanServer mbeanServer,
                                final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
                                final ServiceRegistry serviceRegistry) {

    super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);

    String killHours = System
            .getenv().getOrDefault("AUTO_KILL_SWITCH_TIME_MINUTES", DEFAULT_KILL_DURATION_MINUTES);
    killSwitch = new AutoKillSwitch("ActiveMQ Server Shutdown",
            Duration.ofMinutes(Integer.parseInt(killHours)), () -> this.stop(true));
    kafkaIntegration = new KafkaIntegration(configuration);
    kafkaExchangeManager = new KafkaExchangeManager(
            configuration.getBridgeConfig(), kafkaIntegration.getKafkaIO());
    this.registerBrokerPlugin(kafkaExchangeManager);
  }


  public KafkaExchangeManager getKafkaExchangeManager() {
    return kafkaExchangeManager;
  }

  public KafkaIntegration getKafkaIntegration() {
    return kafkaIntegration;
  }

  public void doStop(boolean failoverOnServerShutdown, boolean isExit) throws Exception {

    super.stop(failoverOnServerShutdown, isExit);

    kafkaExchangeManager.stop();
    kafkaIntegration.stop();
  }

  public BridgeConfig getBridgeConfig() {
    return ((JmsBridgeConfiguration) getConfiguration()).getBridgeConfig();
  }

  @Override
  public void stop(boolean isShutdown) throws Exception {
    super.stop(isShutdown);
    afterStop();
  }

  @Override
  public void stop(boolean failoverOnServerShutdown, boolean criticalIOError, boolean restarting) {
    super.stop(failoverOnServerShutdown, criticalIOError, restarting);
    afterStop();
  }

  public void doStopTheServer(final boolean criticalIOError) {
    Thread thread = new Thread(() -> {
      try {
        this.stop(false, criticalIOError, false);
      } catch (Exception e) {
        SLOG.error(b -> b.event("StopServer").markFailure(), e);
      }
    });

    thread.start();
  }


  public void doFail(boolean failoverOnServerShutdown) throws Exception {
    super.fail(failoverOnServerShutdown);
    afterStop();
  }

  private void afterStop() {
    try {
      kafkaIntegration.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void doStart() throws Exception {
    beforeStart();

    super.start();

    afterStart();
  }

  private void afterStart() {
    //do nothing
  }

  private void beforeStart() {
    //do nothing
  }

  private JmsBridgeConfiguration getJmsBridgeConfiguration() {
    return (JmsBridgeConfiguration) getConfiguration();
  }

  @Override
  protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
    return super.createNodeManager(directory, replicatingBackup);
  }

//  @Override
//  protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
//    NodeManager manager;
//    boolean isPreferredLive = false;
//
//    final JmsBridgeConfiguration configuration = getJmsBridgeConfiguration();
//    final HAPolicyConfiguration.TYPE haType =
//        configuration.getHAPolicyConfiguration() == null
//            ? null
//            : configuration.getHAPolicyConfiguration().getType();
//
//    if (haType == HAPolicyConfiguration.TYPE.SHARED_STORE_MASTER ||
//        haType == HAPolicyConfiguration.TYPE.LIVE_ONLY ||
//        haType == null) {
//        isPreferredLive = true;
//    }
//
//    if (haType == HAPolicyConfiguration.TYPE.SHARED_STORE_MASTER
//        || haType == HAPolicyConfiguration.TYPE.SHARED_STORE_SLAVE) {
//
//      if (replicatingBackup) {
//        SLOG.error(b -> b
//            .event("CreateNodeManager")
//            .markFailure()
//            .message("replicating backup is not necessary while using kafka persistence"));
//        throw new IllegalArgumentException(
//            "replicating backup is not necessary while using kafka persistence");
//      }
//      manager = createKNodeManager(configuration, replicatingBackup, isPreferredLive);
//
//    } else if (haType == null || haType == HAPolicyConfiguration.TYPE.LIVE_ONLY) {
//
//      SLOG.debug(b -> b
//          .event("CreateNodeManager")
//          .message("Detected no Shared Store HA options on Kafka store"));
//
//      //LIVE_ONLY should be the default HA option when HA isn't configured
//      manager = createKNodeManager(configuration, replicatingBackup, isPreferredLive);
//
//    } else {
//      SLOG.error(b -> b
//          .event("CreateNodeManager")
//          .markFailure()
//          .message("Kafka persistence allows only Shared Store HA options"));
//      throw new IllegalArgumentException("Kafka persistence allows only Shared Store HA options");
//    }
//
//    return manager;
//  }
//
//  private NodeManager createKNodeManager(
//      JmsBridgeConfiguration jmsBridgeConfiguration, boolean replicatedBackup, boolean isPreferredLive) {
//
//    return new KafkaNodeManager(
//            jmsBridgeConfiguration.getBridgeConfig().haConfig(),
//            kafkaIntegration.getNodeUuid(),
//            replicatedBackup);
//  }

  @Override
  protected StorageManager createStorageManager() {
    JmsBridgeConfiguration jmsBridgeConfiguration = getJmsBridgeConfiguration();
    final KafkaJournalStorageManager journal = new KafkaJournalStorageManager(
        kafkaIntegration,
        jmsBridgeConfiguration,
        getCriticalAnalyzer(),
        executorFactory,
        scheduledPool,
        ioExecutorFactory);

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
}
