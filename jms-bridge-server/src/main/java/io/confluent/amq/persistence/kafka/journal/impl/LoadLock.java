/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.inferred.freebuilder.FreeBuilder;

/**
 * A global barrier used to prevent other activities from proceeding before the loading of all data
 * is complete. This lock is based on whether the write ahead log has been fully processed or not
 * and the results have been stored locally for loading.
 */
public class LoadLock {

  private final CountDownLatch latch = new CountDownLatch(1);
  private final HashSet<String> lockSet = new HashSet<>();
  private final Map<String, Map<Integer, Long>> highWaterMarks;
  private volatile boolean locked = true;

  public LoadLock(Consumer<Spec.Builder> specWriter) {
    Spec.Builder specBldr = new Spec.Builder();
    specWriter.accept(specBldr);
    Spec spec = specBldr.build();

    spec.highWaterMarks().stream().map(this::lockKey).forEach(this::addLoadCondition);
    this.highWaterMarks = new HashMap<>();
    spec.highWaterMarks().forEach(tpo -> {
      addLoadCondition(lockKey(tpo));
      this.highWaterMarks.computeIfAbsent(tpo.topic(), topic -> new HashMap<>())
          .put(tpo.partition(), tpo.offset());
    });

  }

  public void addLoadCondition(String topic, int partition, long offset, JournalEntry entry) {
    if (!locked) {
      return;
    }

    if (isBelowHwMark(topic, partition, offset)
        && KafkaRecordUtils.isTxTerminalRecord(entry.getAppendedRecord())) {

      addLoadCondition(lockKey(entry));
    }
  }

  private synchronized void addLoadCondition(String key) {
    if (!locked) {
      throw new IllegalStateException("Lock has already been broken.");
    }

    lockSet.add(key);
  }

  private boolean isBelowHwMark(String topic, int partition, long offset) {
    return offset < this.highWaterMarks
        .getOrDefault(topic, Collections.emptyMap())
        .getOrDefault(partition, Long.MAX_VALUE);
  }

  /**
   * Return true if the lock is broken by this action;
   */
  public boolean removeLoadCondition(
      String topic, int partition, long offset, JournalEntryKey key) {

    return removeLoadCondition(lockKey(topic, partition, offset))
        || removeLoadCondition(key);
  }

  /**
   * Return true if the lock is broken by this action;
   */
  public boolean removeLoadCondition(JournalEntryKey key) {
    return removeLoadCondition(lockKey(key));
  }

  private synchronized boolean removeLoadCondition(String key) {

    lockSet.remove(key);
    if (lockSet.isEmpty() && locked) {
      locked = false;
      latch.countDown();
    }

    return !locked;
  }

  public void waitOnLoad() throws InterruptedException {
    latch.await();
  }

  public boolean loadCompleted() {
    return !locked;
  }

  private String lockKey(TopicPartitionOffset mark) {
    return lockKey(mark.topic(), mark.partition(), mark.offset());
  }

  private String lockKey(String topic, int partition, long offset) {
    return topic + "_" + partition + "_" + offset;
  }

  private String lockKey(JournalEntry entry) {
    return lockKey(KafkaRecordUtils.keyFromEntry(entry));
  }

  private String lockKey(JournalEntryKey key) {
    return key.getTxId() + "_" + key.getMessageId() + "_" + key.getExtendedId();
  }

  @FreeBuilder
  public interface Spec {

    Set<TopicPartitionOffset> highWaterMarks();

    class Builder extends LoadLock_Spec_Builder {

      public Builder addHwMark(String topic, int partition, long offset) {
        this.addHighWaterMarks(new TopicPartitionOffset.Builder().make(topic, partition, offset));
        return this;
      }
    }
  }

  @FreeBuilder
  public interface TopicPartitionOffset {

    String topic();

    int partition();

    long offset();

    class Builder extends LoadLock_TopicPartitionOffset_Builder {

      public TopicPartitionOffset make(String topic, int partition, long offset) {
        return this.topic(topic).partition(partition).offset(offset).build();
      }

    }
  }

}
