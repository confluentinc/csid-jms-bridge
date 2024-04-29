/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.persistence.domain.proto.JournalRecordType.ADD_RECORD;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.EpochEvent;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class KafkaJournalLoader {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaJournalLoader.class));

  private final Set<Integer> epochWaitCount = new HashSet<>();
  private final CompletableFuture<Void> onLoadCompleteFuture = new CompletableFuture<>();
  private final AtomicBoolean isLoadComplete = new AtomicBoolean(false);

  private final String journalName;
  private final EpochCoordinator epochCoordinator;

  public KafkaJournalLoader(
      String journalName, int partitionCount, EpochCoordinator epochCoordinator) {

    this.journalName = journalName;
    this.epochCoordinator = epochCoordinator;

    for (int i = 0; i < partitionCount; i++) {
      epochWaitCount.add(i);
    }
  }

  public boolean isLoadComplete() {
    return isLoadComplete.get();
  }

  public CompletableFuture<Void> onLoadComplete() {
    return onLoadCompleteFuture;
  }

  public synchronized boolean maybeComplete(EpochEvent event) {
    if (!isLoadComplete.get()) {
      epochCoordinator.waitForEpochStart();

      SLOG.debug(b -> b
          .name(journalName)
          .event("MaybeComplete")
          .addEpochEvent(event));

      if (event.getEpochId() >= epochCoordinator.initialEpochId()) {
        if (EpochCoordinator.EPOCH_STAGE_START.equals(event.getEpochStage())) {
          epochWaitCount.add(event.getPartition());

          SLOG.debug(b -> b
              .name(journalName)
              .event("PartitionStarted")
              .putTokens("partition", event.getPartition()));
        } else if (EpochCoordinator.EPOCH_STAGE_READY.equals(event.getEpochStage())) {
          epochWaitCount.remove(event.getPartition());
          SLOG.debug(b -> b
              .name(journalName)
              .event("PartitionFinished")
              .putTokens("partition", event.getPartition()));

          if (epochWaitCount.isEmpty()) {
            isLoadComplete.set(true);
            onLoadCompleteFuture.complete(null);

            SLOG.debug(b -> b
                .name(journalName)
                .event("LoadComplete"));
          }
        }
      }
    }
    return isLoadComplete.get();
  }

  @SuppressWarnings({"CyclomaticComplexity"})
  public void executeLoadCallback(
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store,
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> txStore,
      KafkaJournalLoaderCallback callback) {

    SLOG.info(b -> b
        .name(journalName)
        .event("LoadJournal")
        .markStarted());

    int recordCount = 0;

    List<AnnotationReference> annotations = new LinkedList<>();
    try (KeyValueIterator<JournalEntryKey, JournalEntry> kvIter = store.all()) {
      //only add messages and annotations
      while (kvIter.hasNext()) {

        KeyValue<JournalEntryKey, JournalEntry> kv = kvIter.next();
        JournalEntry entry = kv.value;

        if (entry != null) {
          if (entry.hasAnnotationReference()) {
            annotations.add(entry.getAnnotationReference());
          } else if (entry.getAppendedRecord().getRecordType() == ADD_RECORD) {
            SLOG.trace(b -> b
                .event("LoadAddRecord")
                .addJournalEntryKey(kv.key)
                .addJournalEntry(entry));
            callback.addRecord(KafkaRecordUtils.toRecordInfo(entry.getAppendedRecord()));

            recordCount++;
          }
        }
      }
    }

    //process annotations
    loadAnnotations(store, callback, annotations);
    loadTransactions(txStore, callback);

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

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void loadTransactions(
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store,
      KafkaJournalLoaderCallback callback) {

    List<JournalEntry> txRefList = new LinkedList<>();

    while (true) {
      try {
        try (KeyValueIterator<JournalEntryKey, JournalEntry> kvIter = store.all()) {
          while (kvIter.hasNext()) {

            KeyValue<JournalEntryKey, JournalEntry> kv = kvIter.next();
            JournalEntry entry = kv.value;
            if (entry.hasTransactionReference()) {
              txRefList.add(entry);
            }
          }
        }
        break;
      } catch (InvalidStateStoreException ignored) {
        // store not yet ready for querying
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }


    for (JournalEntry txRefEntry : txRefList) {
      TransactionReference txref = txRefEntry.getTransactionReference();
      List<RecordInfo> txRecords = new LinkedList<>();
      List<RecordInfo> txDeleteRecords = new LinkedList<>();
      boolean prepared = false;
      byte[] txData = null;

      for (JournalEntryKey refKey : txref.getEntryReferencesList()) {
        JournalEntry entry = store.get(refKey);

        if (entry == null) {
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
        }
      }

      if (prepared) {
        SLOG.warn(b -> b
            .name(journalName)
            .event("PreparedTransactionLoaded")
            .addJournalEntry(txRefEntry));

        PreparedTransactionInfo pti = new PreparedTransactionInfo(txref.getTxId(), txData);
        pti.getRecords().addAll(txRecords);
        pti.getRecordsToDelete().addAll(txDeleteRecords);
        callback.addPreparedTransaction(pti);
      }
    }
  }
}
