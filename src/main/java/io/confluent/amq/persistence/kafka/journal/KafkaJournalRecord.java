/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.kafka.JournalPartitionKey;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import org.apache.activemq.artemis.core.journal.IOCompletion;

/**
 * A record that has been read from the kafka journal.
 */
public class KafkaJournalRecord {

  // Below are the journal record types
  /*
    ADD and UPDATE both require

      long id -> id,
      byte recordType -> userRecordType
      Persister persister: used to get serialize the record Object,
      Object record -> data
   */
  static final byte ADD_RECORD = 11;
  static final byte UPDATE_RECORD = 12;

  /*
   *  ADD TX and UPDATE TX both require
   *
      long txID -> txId
      long id -> id,
      Persister persister: used to get serialize the record Object,
      Object record -> data
      byte recordType -> userRecordType
   */
  static final byte ADD_RECORD_TX = 13;
  static final byte UPDATE_RECORD_TX = 14;

  /*
    DELETE require
    long id -> id
   */
  static final byte DELETE_RECORD_TX = 15;

  /*
    DELETE TX requires

     long txID -> txId
     long id -> id
     EncodingSupport record -> data
   */
  static final byte DELETE_RECORD = 16;

  /*
   PREPARE and COMMIT

    long txId -> txId
    int numberOfRecords: 0 for prepare
    EncodingSupport txData -> txData
   */
  static final byte PREPARE_RECORD = 17;
  static final byte COMMIT_RECORD = 18;

  /*
    ROLLBACK

    long txId -> txId
   */
  static final byte ROLLBACK_RECORD = 19;

  private final JournalRecord record;
  private IOCompletion ioCompletion;
  private boolean sync;
  private String destTopic;

  public KafkaJournalRecord(JournalRecord record) {
    this.record = record;
  }

  public JournalRecordKey getKafkaMessageKey() {
    return JournalRecordKey.newBuilder()
        .setId(record.getId())
        .setTxId(record.getTxId())
        .build();
  }

  public JournalPartitionKey getKafkaPartitionKey() {
    switch (record.getRecordType()) {
      case ADD_RECORD_TX:
      case DELETE_RECORD_TX:
      case UPDATE_RECORD_TX:
      case PREPARE_RECORD:
      case ROLLBACK_RECORD:
        return JournalPartitionKey.newBuilder().setId(record.getTxId()).build();
      default:
        return JournalPartitionKey.newBuilder().setId(record.getId()).build();
    }
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

