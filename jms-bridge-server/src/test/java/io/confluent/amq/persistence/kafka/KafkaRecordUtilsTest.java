/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import org.junit.jupiter.api.Test;

import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaRecordUtilsTest {

  @Test
  void testKeyBuilder() {
    JournalRecord record = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE)
        .setTxId(2L)
        .setMessageId(3L)
        .build();
    JournalEntryKey key = KafkaRecordUtils.keyFromRecord(record);
    assertNotNull(key);
    assertEquals(key.getTxId(), 2L);
    assertEquals(key.getMessageId(), 3L);
  }

}