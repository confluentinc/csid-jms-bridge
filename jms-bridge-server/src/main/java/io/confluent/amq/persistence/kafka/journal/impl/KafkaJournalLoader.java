/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import java.util.LinkedList;
import java.util.List;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class KafkaJournalLoader {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaJournalLoader.class));

  private ProcessorContext currContext;

  private final String journalName;

  public KafkaJournalLoader(
      String journalName) {

    this.journalName = journalName;
  }

  @SuppressWarnings({"CyclomaticComplexity"})
  public void executeLoadCallback(
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store,
      KafkaJournalLoaderCallback callback) {

    SLOG.info(b -> b
        .name(journalName)
        .event("LoadJournal")
        .markStarted());

    int recordCount = 0;

    List<TransactionReference> foundTransactions = new LinkedList<>();
    List<AnnotationReference> annotations = new LinkedList<>();
    KeyValueIterator<JournalEntryKey, JournalEntry> kvIter = store.all();
    if (kvIter != null) {
      try {
        //only add messages and annotations
        while (kvIter.hasNext()) {

          KeyValue<JournalEntryKey, JournalEntry> kv = kvIter.next();
          JournalEntry entry = kv.value;

          if (entry != null) {
            if (entry.hasAnnotationReference()) {
              annotations.add(entry.getAnnotationReference());

            } else if (entry.hasTransactionReference()) {
              foundTransactions.add(entry.getTransactionReference());

            } else {
              switch (entry.getAppendedRecord().getRecordType()) {
                case ADD_RECORD:
                  SLOG.trace(b -> b
                      .event("LoadAddRecord")
                      .addJournalEntryKey(kv.key)
                      .addJournalEntry(entry));
                  callback.addRecord(KafkaRecordUtils.toRecordInfo(entry.getAppendedRecord()));

                  recordCount++;
                  break;
                case DELETE_RECORD:
                  //this does not need to be recorded since it overwrote the ADD_RECORD already
                  //
                  //callback.deleteRecord(entry.getAppendedRecord().getMessageId());
                  //recordCount--;
                  SLOG.trace(b -> b
                      .event("LoadDeleteRecord")
                      .addJournalEntryKey(kv.key)
                      .addJournalEntry(entry));
                  break;
                case ANNOTATE_RECORD:
                  //unprocessed annotation
                  //callback.updateRecord(KafkaRecordUtils.toRecordInfo(entry.getAppendedRecord()));
                  break;
                default:
                  //skip it, don't care
              }
            }
          }
        }
      } finally {
        kvIter.close();
      }
    }
    //process annotations
    loadAnnotations(store, callback, annotations);
    loadTransactions(store, callback, foundTransactions);

    final int finalLoadCount = recordCount;
    callback.loadComplete(finalLoadCount);

    SLOG.info(b -> b
        .name(journalName)
        .event("LoadJournal")
        .markCompleted()
        .putTokens("finalLoadCount", finalLoadCount));
  }

  private void loadAnnotations(
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store,
      KafkaJournalLoaderCallback callback,
      List<AnnotationReference> annRefList) {

    for (AnnotationReference annRef : annRefList) {

      for (JournalEntryKey annRefKey : annRef.getEntryReferencesList()) {

        JournalEntry annEntry = store.get(annRefKey);

        if (annEntry != null) {
          SLOG.trace(b -> b
              .event("LoadAnnotation")
              .addJournalEntry(annEntry));

          callback.updateRecord(KafkaRecordUtils.toRecordInfo(annEntry.getAppendedRecord()));
        } else {
          SLOG.warn(b -> b
              .event("LoadAnnotation")
              .markFailure()
              .message("No record found for annotation reference"));
        }
      }
    }
  }

  private void loadTransactions(
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store,
      KafkaJournalLoaderCallback callback,
      List<TransactionReference> txRefList) {

    for (TransactionReference txref : txRefList) {
      List<RecordInfo> txRecords = new LinkedList<>();
      List<RecordInfo> txDeleteRecords = new LinkedList<>();
      boolean prepared = false;
      byte[] txData = null;

      for (JournalEntryKey refKey : txref.getEntryReferencesList()) {
        JournalEntry entry = store.get(refKey);

        if (entry == null) {
          //todo: Fix this, these shouldn't be loaded at all.
          SLOG.warn(b -> b
              .name(journalName)
              .event("InvalidTransactionReference")
              .addJournalEntryKey(refKey)
              .message("No value found in store for transaction reference."));

        } else {

          switch (entry.getAppendedRecord().getRecordType()) {
            case PREPARE_TX:
              prepared = true;
              if (!entry.getAppendedRecord().getData().isEmpty()) {
                txData = entry.getAppendedRecord().getData().toByteArray();
              }
              break;
            case ADD_RECORD_TX:
            case ANNOTATE_RECORD_TX:
              txRecords.add(KafkaRecordUtils.toRecordInfo(entry.getAppendedRecord()));
              break;
            case DELETE_RECORD_TX:
              txDeleteRecords.add(KafkaRecordUtils.toRecordInfo(entry.getAppendedRecord()));
              break;
            default:
              //ignore, do nothing
          }

          if (prepared) {
            PreparedTransactionInfo pti = new PreparedTransactionInfo(txref.getTxId(), txData);
            pti.getRecords().addAll(txRecords);
            pti.getRecordsToDelete().addAll(txDeleteRecords);
          } else {
            callback.failedTransaction(txref.getTxId(), txRecords, txDeleteRecords);
          }
        }
      }
    }
  }
}
