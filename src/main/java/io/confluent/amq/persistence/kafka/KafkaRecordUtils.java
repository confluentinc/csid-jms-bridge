/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import org.apache.activemq.artemis.core.journal.RecordInfo;

public final class KafkaRecordUtils {

  private KafkaRecordUtils() {
  }

  public static RecordInfo toRecordInfo(JournalRecord jrec) {
    if (jrec == null) {
      return null;
    }

    boolean isUpdate = jrec.getRecordType() == JournalRecordType.UPDATE_RECORD
        || jrec.getRecordType() == JournalRecordType.UPDATE_RECORD_TX;

    return new RecordInfo(jrec.getId(), (byte) jrec.getUserRecordType(),
        jrec.getData().toByteArray(), isUpdate, (short) 0);
  }

}
