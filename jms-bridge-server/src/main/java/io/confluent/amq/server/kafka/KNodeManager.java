/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import java.io.File;
import java.io.IOException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
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
  private volatile boolean interrupted = false;

  public long failoverPause = 0L;

  public KNodeManager(
      JmsBridgeConfiguration config, KafkaIntegration kafkaIntegration, boolean replicatedBackup) {
    this(config, kafkaIntegration, replicatedBackup, null);
  }

  public KNodeManager(
      JmsBridgeConfiguration config,
      KafkaIntegration kafkaIntegration,
      boolean replicatedBackup,
      File directory) {

    super(replicatedBackup, directory);
    setUUID(kafkaIntegration.getNodeUuid());
    this.kafkaIntegration = kafkaIntegration;
    this.config = config;
  }

  @Override
  public synchronized void start() throws Exception {
    super.start();
    kafkaIntegration.start();
    SLOG.info(
        b -> b.event("StartedNodeManager").markSuccess());
  }

  @Override
  public synchronized void stop() throws Exception {
    super.stop();
    SLOG.info(b -> b.event("StoppedNodeManager"));
  }

  private void checkInterrupted() throws InterruptedException {
    if (this.interrupted) {
      interrupted = false;
      throw new InterruptedException("KNodeManager was interrupted");
    }
  }

  @Override
  public void awaitLiveNode() throws Exception {
    SLOG.info(b -> b.event("AwaitLiveNode"));
    kafkaIntegration.waitForProcessorObtainPartition();
    if (failoverPause > 0L) {
      Thread.sleep(failoverPause);
    }
  }

  @Override
  public void awaitLiveStatus() throws Exception {
    SLOG.info(b -> b.event("AwaitLiveStatus"));
    while (state != State.LIVE) {
      Thread.sleep(10);
    }
  }

  @Override
  public void startBackup() throws Exception {
    SLOG.info(b -> b.event("StartBackup"));
  }

  @Override
  public ActivateCallback startLiveNode() throws Exception {
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
  public void pauseLiveServer() throws Exception {
    state = State.PAUSED;
    SLOG.info(b -> b.event("PauseLiveServer"));
    kafkaIntegration.stopProcessor();
  }

  @Override
  public void crashLiveServer() throws Exception {
    SLOG.info(b -> b.event("CrashLiveServer"));
    kafkaIntegration.stopProcessor();
  }

  @Override
  public boolean isAwaitingFailback() throws Exception {
    SLOG.info(
        b -> b.event("IsAwaitingFallback"));
    return state == State.FAILING_BACK;
  }

  @Override
  public boolean isBackupLive() throws Exception {
    ConsumerGroupDescription groupDescription = kafkaIntegration.getKafkaIO()
        .describeConsumerGroup(kafkaIntegration.getApplicationId());
    boolean isLive = groupDescription.members().size() > 1;
    SLOG.info(b -> b.event("IsBackupLive").putTokens("isLive", isLive));
    return isLive;
  }

  @Override
  public void interrupt() {
    this.interrupted = true;
  }

  @Override
  public void releaseBackup() throws Exception {
    //
  }

  public void reset() throws Exception {
    super.stop();
    state = State.NOT_STARTED;
  }

  @Override
  public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
    return getNodeId();
  }
}

