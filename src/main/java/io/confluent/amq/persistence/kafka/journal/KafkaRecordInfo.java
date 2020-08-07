/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import org.apache.activemq.artemis.core.journal.RecordInfo;

public class KafkaRecordInfo {
  private final JournalRecord journalRecord;
  private final byte[] kafkaKey;
  private final boolean delete;

  public KafkaRecordInfo(byte[] kafkaKey, JournalRecord journalRecord) {
    this(kafkaKey, journalRecord, false);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public KafkaRecordInfo(byte[] kafkaKey, JournalRecord journalRecord, boolean isDelete) {
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

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public byte[] getKafkaKey() {
    return kafkaKey;
  }

  public RecordInfo toRecordInfo() {
    return KafkaRecordUtils.toRecordInfo(journalRecord);
  }
}
