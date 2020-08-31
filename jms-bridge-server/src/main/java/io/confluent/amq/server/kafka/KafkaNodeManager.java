/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.persistence.kafka.journal.KJournalAssignment;
import io.confluent.amq.persistence.kafka.journal.KJournalListener;
import io.confluent.amq.persistence.kafka.journal.KJournalMetadata;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaNodeManager extends NodeManager implements KJournalListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNodeManager.class);
  private final AtomicBoolean isAlive = new AtomicBoolean(false);
  private final AtomicBoolean hasLock = new AtomicBoolean(false);
  private final AtomicReference<SimpleString> nodeId = new AtomicReference<>(null);

  public KafkaNodeManager() {
    super(false, null);
  }

  @Override
  public void awaitLiveNode() throws Exception {
    LOGGER.debug("ENTER awaitLiveStatus");
    while (!isAlive.get() | !hasLock.get()) {
      Thread.sleep(50);
    }
  }

  @Override
  public void awaitLiveStatus() throws Exception {
    LOGGER.debug("ENTER awaitLiveStatus");
    while (!isAlive.get()) {
      Thread.sleep(50);
    }
  }

  @Override
  public void startBackup() throws Exception {
    LOGGER.debug("ENTER startBackup");
    // Look into implementation of scheduledBackupLock
  }

  @Override
  public ActivateCallback startLiveNode() throws Exception {
    LOGGER.debug("ENTER startLiveNode");

    return new ActivateCallback() {
    };
  }

  @Override
  public void pauseLiveServer() throws Exception {
    LOGGER.debug("Paused LiveServer");
    close();
  }

  @Override
  public void crashLiveServer() throws Exception {
    LOGGER.debug("Paused LiveServer");
    close();
  }

  private void close() {
    hasLock.set(false);
    isAlive.set(false);
  }

  @Override
  public void releaseBackup() throws Exception {
    LOGGER.debug("Backup Lock Released");
  }

  @Override
  public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
    return getNodeId();
  }

  @Override
  public SimpleString getNodeId() {
    return Optional.ofNullable(nodeId.get()).orElse(SimpleString.toSimpleString("UNKNOWN"));
  }

  @Override
  public boolean isAwaitingFailback() throws Exception {
    return false;
  }

  @Override
  public boolean isBackupLive() throws Exception {
    return true;
  }

  @Override
  public void interrupt() {
    //
  }

  @Override
  public void onAssignmentChange(
      KJournalMetadata metadata, List<KJournalAssignment> assignmentList) {

    if (KafkaJournalStorageManager.BINDINGS_NAME.equals(metadata.journalName())) {
      boolean locked = assignmentList
          .stream()
          .filter(a -> a.journalName().equals(KafkaJournalStorageManager.BINDINGS_NAME))
          .anyMatch(a -> a.partition() == 0);
      hasLock.set(locked);
    }

    nodeId.compareAndSet(null, SimpleString.toSimpleString(metadata.nodeId()));
  }

  @Override
  public void onStateChange(
      KJournalMetadata metadata, KJournalState oldState, KJournalState newState) {

    if (KafkaJournalStorageManager.BINDINGS_NAME.equals(metadata.journalName())
        && newState == KJournalState.RUNNING) {

      LOGGER.debug("Node Manager released state");
      isAlive.set(true);

    } else {
      isAlive.set(false);
    }

  }

}
