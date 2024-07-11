package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoader;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoaderCallback;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;

import java.util.LinkedList;
import java.util.List;

import static io.confluent.amq.persistence.domain.proto.JournalRecordType.ADD_RECORD;

public class KCacheJournalLoader {
    private final String journalName;
    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(KafkaJournalLoader.class));

    public KCacheJournalLoader(String journalName) {
        this.journalName = journalName;
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    public void executeLoadCallback(
            JournalCache journalCache,
            KafkaJournalLoaderCallback callback) {

        SLOG.info(b -> b
                .name(journalName)
                .event("LoadJournal")
                .markStarted());

        int recordCount = 0;

        List<AnnotationReference> annotations = new LinkedList<>();
        try (KeyValueIterator<JournalEntryKey, JournalEntry> kvIter = journalCache.getCache().all()) {
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
        loadAnnotations(journalCache, callback, annotations);
        loadTransactions(journalCache, callback);

        final int finalLoadCount = recordCount;
        callback.loadComplete(finalLoadCount);

        SLOG.info(b -> b
                .name(journalName)
                .event("LoadJournal")
                .markCompleted()
                .putTokens("finalLoadCount", finalLoadCount));
    }

    private void loadAnnotations(
            JournalCache journalCache,
            KafkaJournalLoaderCallback callback,
            List<AnnotationReference> annRefList) {

        for (AnnotationReference annRef : annRefList) {

            for (JournalEntryKey annRefKey : annRef.getEntryReferencesList()) {

                JournalEntry annEntry = journalCache.getCache().get(annRefKey);

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
            JournalCache journalCache,
            KafkaJournalLoaderCallback callback) {

        List<JournalEntry> txRefList = new LinkedList<>();

        int retryAttempt = 0;
        int maxRetries = 5;
        while (true) {
            try {
                try (KeyValueIterator<JournalEntryKey, JournalEntry> kvIter = journalCache.getCache().all()) {
                    while (kvIter.hasNext()) {

                        KeyValue<JournalEntryKey, JournalEntry> kv = kvIter.next();
                        JournalEntry entry = kv.value;
                        if (entry.hasTransactionReference()) {
                            txRefList.add(entry);
                        }
                    }
                }
                break;
            } catch (Exception ignored) {
                //todo: do nothing?
            }
        }


        for (JournalEntry txRefEntry : txRefList) {
            TransactionReference txref = txRefEntry.getTransactionReference();
            List<RecordInfo> txRecords = new LinkedList<>();
            List<RecordInfo> txDeleteRecords = new LinkedList<>();
            boolean prepared = false;
            byte[] txData = null;

            for (JournalEntryKey refKey : txref.getEntryReferencesList()) {
                JournalEntry entry = journalCache.getCache().get(refKey);

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
