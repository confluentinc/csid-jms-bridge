/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import org.apache.activemq.artemis.core.journal.RecordInfo;

public class KafkaRecordInfo {

  private final JournalRecord journalRecord;
  private final JournalEntryKey kafkaKey;
  private final boolean delete;

  public KafkaRecordInfo(JournalEntryKey kafkaKey, JournalRecord journalRecord) {
    this(kafkaKey, journalRecord, false);
  }

  public KafkaRecordInfo(JournalEntryKey kafkaKey, JournalRecord journalRecord, boolean isDelete) {
    this.journalRecord = journalRecord;
    this.kafkaKey = kafkaKey;
    this.delete = isDelete;
  }

  public boolean isDelete() {
    return delete;
  }

  public JournalRecord getJournalRecord() {
    return journalRecord;
  }

  public JournalEntryKey getKafkaKey() {
    return kafkaKey;
  }

  public RecordInfo toRecordInfo() {
    return KafkaRecordUtils.toRecordInfo(journalRecord);
  }
}
