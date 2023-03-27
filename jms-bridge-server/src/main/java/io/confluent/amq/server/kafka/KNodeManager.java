/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;

public class KNodeManager extends NodeManager {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KNodeManager.class));

  public enum State {
    LIVE, PAUSED, FAILING_BACK, NOT_STARTED
  }

  private final JmsBridgeConfiguration config;
  private final KafkaIntegration kafkaIntegration;

  private volatile State state = State.NOT_STARTED;

  public long failoverPause = 0L;

  public KNodeManager(
      JmsBridgeConfiguration config,
      KafkaIntegration kafkaIntegration,
      boolean replicatedBackup) {

    super(replicatedBackup);
    setUUID(kafkaIntegration.getNodeUuid());
    this.kafkaIntegration = kafkaIntegration;
    this.config = config;
  }

  @Override
  public synchronized void start() throws Exception {
    super.start();
    SLOG.info(
        b -> b.event("StartedNodeManager").markSuccess());
  }

  @Override
  public synchronized void stop() throws Exception {
    super.stop();
    SLOG.info(b -> b.event("StoppedNodeManager"));
  }

  @Override
  public void awaitLiveNode() {
    SLOG.info(b -> b.event("AwaitLiveNode"));

    try {
      kafkaIntegration.waitForProcessorObtainPartition();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (failoverPause > 0L) {
      try {
        Thread.sleep(failoverPause);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void awaitLiveStatus() {
    SLOG.info(b -> b.event("AwaitLiveStatus"));
    while (state != State.LIVE) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void startBackup() {
    SLOG.info(b -> b.event("StartBackup"));
  }

  @Override
  public ActivateCallback startLiveNode() {
    SLOG.info(
        b -> b.event("StartLiveNode"));
    state = State.FAILING_BACK;
    //kafkaIntegration.waitForProcessorObtainPartition();
    return new CleaningActivateCallback() {
      @Override
      public void activationComplete() {
        try {
          state = State.LIVE;
        } catch (Exception e) {
          SLOG.warn(b -> b.event("StartLiveNode").markFailure(), e);
        }
      }
    };
  }

  @Override
  public void pauseLiveServer() {
    state = State.PAUSED;
    SLOG.info(b -> b.event("PauseLiveServer"));
    try {
      kafkaIntegration.stopProcessor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void crashLiveServer() {
    SLOG.info(b -> b.event("CrashLiveServer"));
    try {
      kafkaIntegration.stopProcessor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isAwaitingFailback() {
    SLOG.info(
        b -> b.event("IsAwaitingFallback"));
    return state == State.FAILING_BACK;
  }

  @Override
  public boolean isBackupLive() {
    ConsumerGroupDescription groupDescription = kafkaIntegration.getKafkaIO()
        .describeConsumerGroup(kafkaIntegration.getApplicationId());
    boolean isLive = groupDescription.members().size() > 1;
    SLOG.info(b -> b.event("IsBackupLive").putTokens("isLive", isLive));
    return isLive;
  }

  public void interrupt() {
    //
  }

  @Override
  public void releaseBackup() {
    //
  }

  public void reset() throws Exception {
    super.stop();
    state = State.NOT_STARTED;
  }

  @Override
  public SimpleString readNodeId() {
    return getNodeId();
  }
}

