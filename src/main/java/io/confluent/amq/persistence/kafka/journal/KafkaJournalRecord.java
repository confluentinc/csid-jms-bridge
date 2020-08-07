/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import org.apache.activemq.artemis.core.journal.IOCompletion;

/**
 * A record that has been read from the kafka journal.
 */
public class KafkaJournalRecord {

  private final JournalRecord record;
  private IOCompletion ioCompletion;
  private boolean sync;
  private String destTopic;

  public KafkaJournalRecord(JournalRecord record) {
    this.record = record;
  }

  public JournalRecordKey getKafkaMessageKey() {

    boolean update =
        record.getRecordType() == JournalRecordType.UPDATE_RECORD_TX
          ||
        record.getRecordType() == JournalRecordType.UPDATE_RECORD;

    return JournalRecordKey.newBuilder()
        .setId(record.getId())
        .setTxId(record.getTxId())
        .setUpdate(update)
        .build();
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

  public IOCompletion getIoCompletion() {
    return ioCompletion;
  }

  public KafkaJournalRecord setIoCompletion(
      IOCompletion ioCompletion) {
    this.ioCompletion = ioCompletion;
    return this;
  }
}

