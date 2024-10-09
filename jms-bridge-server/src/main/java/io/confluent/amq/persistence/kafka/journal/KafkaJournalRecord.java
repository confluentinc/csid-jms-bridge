/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import org.apache.activemq.artemis.core.journal.IOCompletion;

/**
 * A record that has been read from the kafka journal.
 */
public class KafkaJournalRecord {

  private final JournalRecord record;
  private IOCompletion ioCompletion;
  private boolean sync;
  private boolean storeLineUp = true;
  private String destTopic;

  public KafkaJournalRecord(JournalRecord record) {
    this.record = record;
  }

  public JournalEntryKey getKafkaMessageKey() {
    return KafkaRecordUtils.keyFromRecord(record);
  }

  public String getDestTopic() {
    return destTopic;
  }

  public JournalRecord getRecord() {
    return record;
  }

  public KafkaJournalRecord setDestTopic(String destTopic) {
    this.destTopic = destTopic;
    return this;
  }

  public boolean isSync() {
    return sync;
  }

  public KafkaJournalRecord setSync(boolean sync) {
    this.sync = sync;
    return this;
  }

  public boolean isStoreLineUp() {
    return storeLineUp;
  }

  public KafkaJournalRecord setStoreLineUp(boolean storeLineUp) {
    this.storeLineUp = storeLineUp;
    return this;
  }

  public IOCompletion getIoCompletion() {
    return ioCompletion;
  }

  public KafkaJournalRecord setIoCompletion(
      IOCompletion ioCompletion) {
    this.ioCompletion = ioCompletion;
    return this;
  }
}

