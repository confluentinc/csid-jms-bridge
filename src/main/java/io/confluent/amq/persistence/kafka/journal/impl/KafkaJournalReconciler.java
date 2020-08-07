/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.persistence.kafka.ReconciledMessage;
import java.util.Collections;
import java.util.List;

public class KafkaJournalReconciler {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaJournalReconciler.class));

  private final KafkaJournalHandler recordHandler;
  private final KafkaJournalTxHandler txHandler;
  private final String journalTopic;

  public KafkaJournalReconciler(
      KafkaJournalTxHandler txHandler, String journalTopic) {
    this(KafkaJournalHandler.NO_OP, txHandler, journalTopic);
  }

  public KafkaJournalReconciler(
      KafkaJournalHandler recordHandler,
      KafkaJournalTxHandler txHandler, String journalTopic) {

    this.recordHandler = recordHandler;
    this.txHandler = txHandler;
    this.journalTopic = journalTopic;
  }

  public List<ReconciledMessage<?>> deleteRecord(JournalRecordKey key, JournalRecord record) {
    SLOG.trace(b -> {
      b.name(journalTopic)
          .event("Tombstone")
          .addJournalRecordKey(key);
    });

    return Collections.singletonList(ReconciledMessage.tombstone(journalTopic, key));
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public List<ReconciledMessage<?>> reconcileRecord(JournalRecordKey key, JournalRecord record) {
    List<ReconciledMessage<?>> reconciledMessages = Collections.emptyList();
    if (record == null) {
      //tombstone
      return reconciledMessages;
    }

    switch (record.getRecordType()) {
      case ADD_RECORD:
      case UPDATE_RECORD:
        reconciledMessages = recordHandler.handleRecord(key, record);
        break;
      case DELETE_RECORD:
        //the original record, delete record and tombstone record all share the same key
        reconciledMessages = deleteRecord(key, record);
        break;
      case PREPARE_RECORD:
      case UPDATE_RECORD_TX:
      case DELETE_RECORD_TX:
      case ROLLBACK_RECORD:
      case ADD_RECORD_TX:
      case COMMIT_RECORD:
        reconciledMessages = txHandler.handleTxRecord(key, record);
        break;
      case UNRECOGNIZED:
      case UNKNOWN:
      default:
        SLOG.error(b -> b
            .name(journalTopic)
            .event("InvalidRecordType")
            .putTokens("recordType", record.getRecordType()));
        throw new RuntimeException("Invalid record encountered");
    }
    return reconciledMessages;
  }

}
