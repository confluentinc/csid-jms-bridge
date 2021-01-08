/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.domain.proto.MessageAnnotation;
import io.confluent.amq.persistence.kafka.journal.impl.EpochCoordinator;
import java.util.EnumSet;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

public final class KafkaRecordUtils {

  public static final String EPOCH_RECORD_HEADER = "_epoch";
  public static final int MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT = -1;
  public static final int TRANSACTION_REFERENCE_EXTENDED_ID_CONSTANT = -2;

  private static final EnumSet<JournalRecordType> TX_TERM_TYPES = EnumSet.of(
      JournalRecordType.COMMIT_TX,
      JournalRecordType.ROLLBACK_TX);

  private static final EnumSet<JournalRecordType> TX_TYPES = EnumSet.of(
      JournalRecordType.PREPARE_TX,
      JournalRecordType.COMMIT_TX,
      JournalRecordType.ROLLBACK_TX,
      JournalRecordType.ADD_RECORD_TX,
      JournalRecordType.ANNOTATE_RECORD_TX,
      JournalRecordType.DELETE_RECORD_TX);

  private static final EnumSet<JournalRecordType> MSG_TYPES = EnumSet.of(
      JournalRecordType.ADD_RECORD,
      JournalRecordType.ADD_RECORD_TX,
      JournalRecordType.DELETE_RECORD,
      JournalRecordType.DELETE_RECORD_TX,
      JournalRecordType.ANNOTATE_RECORD,
      JournalRecordType.ANNOTATE_RECORD_TX
  );

  private static final EnumSet<JournalRecordType> ANN_TYPES = EnumSet.of(
      JournalRecordType.ANNOTATE_RECORD,
      JournalRecordType.ANNOTATE_RECORD_TX
  );

  private static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
  private static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();

  private KafkaRecordUtils() {
  }

  public static RecordInfo toRecordInfo(Long messageId, MessageAnnotation annotation) {
    if (annotation == null) {
      return null;
    }

    return new RecordInfo(
        messageId,
        (byte) annotation.getProtocolRecordType(),
        annotation.getData().toByteArray(),
        true,
        (short) 0);
  }

  public static RecordInfo toRecordInfo(JournalRecord jrec) {
    if (jrec == null) {
      return null;
    }

    boolean isUpdate = jrec.getRecordType() == JournalRecordType.ANNOTATE_RECORD
        || jrec.getRecordType() == JournalRecordType.ANNOTATE_RECORD_TX;

    return new RecordInfo(jrec.getMessageId(), (byte) jrec.getProtocolRecordType(),
        jrec.getData().toByteArray(), isUpdate, (short) 0);
  }

  public static boolean isTxTerminalRecord(JournalRecord record) {
    return record != null && TX_TERM_TYPES.contains(record.getRecordType());
  }

  public static boolean isTxRecord(JournalRecord record) {
    return record != null && TX_TYPES.contains(record.getRecordType());
  }

  public static boolean isMessageRecord(JournalRecord record) {
    return record != null && MSG_TYPES.contains(record.getRecordType());
  }

  public static boolean isAnnotationRecord(JournalRecord record) {
    return record != null && ANN_TYPES.contains(record.getRecordType());
  }

  @SuppressWarnings("UnstableApiUsage")
  public static JournalEntryKey keyFromRecord(JournalRecord record) {
    JournalEntryKey.Builder keyBuilder = JournalEntryKey.newBuilder();

    if (isTxRecord(record)) {
      keyBuilder.setTxId(record.getTxId());
    }

    if (isMessageRecord(record)) {
      keyBuilder.setMessageId(record.getMessageId());
    }

    if (isAnnotationRecord(record)) {
      HashCode hash = Hashing.murmur3_32().hashBytes(record.toByteArray());
      keyBuilder.setExtendedId(hash.asInt());
    }

    return keyBuilder.build();
  }

  public static JournalEntryKey addDeleteKeyFromMessageId(long messageId) {
    return JournalEntryKey.newBuilder().setMessageId(messageId).build();
  }

  public static JournalEntryKey transactionKeyFromTxId(long txId) {
    return JournalEntryKey.newBuilder().setTxId(txId).build();
  }

  public static JournalEntryKey annotationsKeyFromRecordKey(JournalEntryKey recordKey) {
    return  annotationsKeyFromMessageId(recordKey.getMessageId());
  }

  public static JournalEntryKey annotationsKeyFromMessageId(Long messageId) {
    return JournalEntryKey.newBuilder()
        .setMessageId(messageId)
        .setExtendedId(MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT)
        .build();
  }

  public static JournalEntryKey transactionReferenceKeyFromTxId(Long txId) {
    return JournalEntryKey.newBuilder()
        .setTxId(txId)
        .setExtendedId(TRANSACTION_REFERENCE_EXTENDED_ID_CONSTANT)
        .build();
  }

  public static JournalEntryKey keyFromEntry(JournalEntry journalEntry) {
    if (journalEntry != null) {
      if (journalEntry.hasAppendedRecord()) {
        return keyFromRecord(journalEntry.getAppendedRecord());
      }
    }
    return null;
  }

  public static JournalEntryKey epochKey() {
    return JournalEntryKey.newBuilder()
        .setTxId(-1)
        .setMessageId(-1)
        .setExtendedId(-1)
        .build();
  }

  public static int getEpochHeader(Headers headers) {
    Header hdr =  headers.lastHeader(EPOCH_RECORD_HEADER);
    return hdr != null
        ? INTEGER_DESERIALIZER.deserialize(null, hdr.value())
        : 0;
  }

  public static void addEpochHeader(Headers headers) {
    headers.add(
        EPOCH_RECORD_HEADER,
        INTEGER_SERIALIZER.serialize(null, EpochCoordinator.currentEpochId()));
  }
}
