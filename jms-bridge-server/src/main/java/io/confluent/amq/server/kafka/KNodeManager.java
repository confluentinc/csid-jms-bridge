/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.FAILING_BACK;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.LIVE;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.NOT_STARTED;
import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.PAUSED;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.persistence.kafka.journal.KJournalAssignment;
import io.confluent.amq.persistence.kafka.journal.KJournalListener;
import io.confluent.amq.persistence.kafka.journal.KJournalMetadata;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;

public class KNodeManager extends NodeManager implements KJournalListener {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KNodeManager.class));

  private final Semaphore liveLock;

  private final Semaphore backupLock;

  public enum State {
    LIVE, PAUSED, FAILING_BACK, NOT_STARTED
  }

  public volatile InVMNodeManager.State state = NOT_STARTED;

  public long failoverPause = 0L;

  public KNodeManager(UUID nodeId, boolean replicatedBackup) {
    this(nodeId, replicatedBackup, null);
  }

  public KNodeManager(UUID nodeId, boolean replicatedBackup, File directory) {
    super(replicatedBackup, directory);
    liveLock = new Semaphore(1);
    backupLock = new Semaphore(1);
    setUUID(nodeId);
  }

  @Override
  public void awaitLiveNode() throws Exception {
    do {
      while (state == NOT_STARTED) {
        Thread.sleep(10);
      }

      liveLock.acquire();

      if (state == PAUSED) {
        liveLock.release();
        Thread.sleep(10);
      } else if (state == FAILING_BACK) {
        liveLock.release();
        Thread.sleep(10);
      } else if (state == LIVE) {
        break;
      }
    }
    while (true);
    if (failoverPause > 0L) {
      Thread.sleep(failoverPause);
    }
  }

  @Override
  public void awaitLiveStatus() throws Exception {
    while (state != LIVE) {
      Thread.sleep(10);
    }
  }

  @Override
  public void startBackup() throws Exception {
    backupLock.acquire();
  }

  @Override
  public ActivateCallback startLiveNode() throws Exception {
    state = FAILING_BACK;
    liveLock.acquire();
    return new CleaningActivateCallback() {
      @Override
      public void activationComplete() {
        try {
          state = LIVE;
        } catch (Exception e) {
          SLOG.warn(b -> b
              .event("StartLiveNode")
              .markFailure(), e);
        }
      }
    };
  }

  @Override
  public void pauseLiveServer() throws Exception {
    state = PAUSED;
    liveLock.release();
  }

  @Override
  public void crashLiveServer() throws Exception {
    liveLock.release();
  }

  @Override
  public boolean isAwaitingFailback() throws Exception {
    return state == FAILING_BACK;
  }

  @Override
  public boolean isBackupLive() throws Exception {
    return liveLock.availablePermits() == 0;
  }

  @Override
  public void interrupt() {
    //
  }

  @Override
  public void releaseBackup() {
    backupLock.release();
  }

  @Override
  public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
    return getNodeId();
  }

  @Override
  public void onAssignmentChange(
      KJournalMetadata metadata, List<KJournalAssignment> assignmentList) {

    if (KafkaJournalStorageManager.BINDINGS_NAME.equals(metadata.journalName())) {
      boolean locked = assignmentList
          .stream()
          .filter(a -> a.journalName().equals(KafkaJournalStorageManager.BINDINGS_NAME))
          .anyMatch(a -> a.partition() == 0);
      if (locked) {
        liveLock.release();
      } else {
        if (state == LIVE) {
          state = FAILING_BACK;
          liveLock.tryAcquire();
        }
      }
    }
  }

  @Override
  public void onStateChange(
      KJournalMetadata metadata, KJournalState oldState, KJournalState newState) {
    //
  }
}
