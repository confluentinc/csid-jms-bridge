/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import com.google.protobuf.ByteString;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import java.nio.charset.StandardCharsets;

public final class TestRecordSupport {

  private TestRecordSupport() {
  }

  public static JournalEntryKey makeTxKey(long txId) {
    return JournalEntryKey.newBuilder()
        .setTxId(txId)
        .build();

  }

  public static JournalEntryKey makeTxKey(long txId, long msgId) {
    return JournalEntryKey.newBuilder()
        .setTxId(txId)
        .setMessageId(msgId)
        .build();

  }

  public static JournalEntryKey makeTxKey(long txId, long msgId, int extId) {
    return JournalEntryKey.newBuilder()
        .setTxId(txId)
        .setMessageId(msgId)
        .setExtendedId(extId)
        .build();
  }

  public static JournalEntryKey makeMsgKey(long msgId) {
    return JournalEntryKey.newBuilder()
        .setMessageId(msgId)
        .build();

  }

  public static JournalEntryKey makeMsgKey(long msgId, int extId) {
    return JournalEntryKey.newBuilder()
        .setMessageId(msgId)
        .setExtendedId(extId)
        .build();

  }

  public static JournalEntry makeRecordOfType(
      Long txId, Long msgId, Integer extId, JournalRecordType type) {

    return makeRecordOfType(txId, msgId, extId, type, (byte[]) null);
  }

  public static JournalEntry makeRecordOfType(
      Long txId, Long msgId, Integer extId, JournalRecordType type, String data) {

    return makeRecordOfType(txId, msgId, extId, type, data.getBytes(StandardCharsets.UTF_8));

  }

  public static JournalEntry makeRecordOfType(
      Long txId, Long msgId, Integer extId, JournalRecordType type, byte[] data) {

    JournalRecord.Builder b =  JournalRecord.newBuilder();
    if (txId != null)  {
      b.setTxId(txId);
    }

    if (msgId != null) {
      b.setMessageId(msgId);
    }

    if (extId != null) {
      b.setExtendedId(extId);
    }

    if (data != null) {
      b.setData(ByteString.copyFrom(data));
    }

    return JournalEntry.newBuilder()
        .setAppendedRecord(b
            .setRecordType(type))
        .buildPartial();
  }
}
