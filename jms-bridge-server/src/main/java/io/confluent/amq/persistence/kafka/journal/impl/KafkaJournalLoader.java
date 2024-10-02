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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.amq.persistence.kafka.LoadInitializer;
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
    private final AtomicBoolean isLoadComplete = new AtomicBoolean(false);

    private final String journalName;
    private final LoadInitializer loadInitializer;

    private final Runnable onLoadCompleteCallback;

    private final int partitionCount;

    public KafkaJournalLoader(
            String journalName, int partitionCount, LoadInitializer loadInitializer, Runnable onLoadCompleteCallback) {
        this.onLoadCompleteCallback = onLoadCompleteCallback;

        this.journalName = journalName;
        this.loadInitializer = loadInitializer;
        this.partitionCount = partitionCount;

        for (int i = 0; i < partitionCount; i++) {
            epochWaitCount.add(i);
        }
    }

    public synchronized boolean maybeComplete(EpochEvent event) {
        if (!isLoadComplete.get()) {
            SLOG.debug(b -> b
                    .name(journalName)
                    .event("MaybeComplete")
                    .addEpochEvent(event));

            if (event.getEpochId() == loadInitializer.getEpochMarker()) {
                if (LoadInitializer.EPOCH_STAGE_START.equals(event.getEpochStage())) {
                    epochWaitCount.add(event.getPartition());

                    SLOG.debug(b -> b
                            .name(journalName)
                            .event("PartitionStarted")
                            .putTokens("partition", event.getPartition()));
                } else if (LoadInitializer.EPOCH_STAGE_READY.equals(event.getEpochStage())) {
                    epochWaitCount.remove(event.getPartition());
                    SLOG.debug(b -> b
                            .name(journalName)
                            .event("PartitionFinished")
                            .putTokens("partition", event.getPartition()));

                    if (epochWaitCount.isEmpty()) {
                        isLoadComplete.set(true);
                        onLoadCompleteCallback.run();

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

        int retryAttempt = 0;
        int maxRetries = 5;
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
                if (ignored.getMessage().contains("migrated to another instance")) {
                    SLOG.logger().warn("State store is not being processed by this this instance.", ignored);
                    return;
                } else {
                    if (retryAttempt < maxRetries) {
                        SLOG.logger().warn("State store may not be ready, will retry", ignored);
                        retryAttempt++;
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new RuntimeException(ignored);
                    }
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

    /**
     * Loader is reused during journal reloads -
     * multiple failover on Backup node switching between active and passive states,
     * therefore its state has to be reset prior to reuse.
     */
    public void reset() {
        epochWaitCount.clear();
        for (int i = 0; i < partitionCount; i++) {
            epochWaitCount.add(i);
        }
        isLoadComplete.set(false);
    }
}
