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

        //all annotation references

        //process annotations
        int recordCount = loadAnnotations(journalCache, callback);
        recordCount += loadTransactions(journalCache, callback);

        final int finalLoadCount = recordCount;
        callback.loadComplete(finalLoadCount);

        SLOG.info(b -> b
                .name(journalName)
                .event("LoadJournal")
                .markCompleted()
                .putTokens("finalLoadCount", finalLoadCount));
    }

    private int loadAnnotations(
            JournalCache journalCache,
            KafkaJournalLoaderCallback callback) {

        List<AnnotationReference> reflist = journalCache.getAnnotationRefs();
        int recordCount = 0;
        for (AnnotationReference annRef : reflist) {

            for (JournalEntryKey annRefKey : annRef.getEntryReferencesList()) {

                JournalEntry annEntry = journalCache.getCache().get(annRefKey);

                if (annEntry != null) {
                    SLOG.trace(b -> b
                            .event("LoadAnnotation")
                            .addJournalEntry(annEntry));

                    callback.updateRecord(KafkaRecordUtils.toRecordInfo(annEntry.getAppendedRecord()));
                    recordCount++;
                } else {
                    SLOG.warn(b -> b
                            .event("LoadAnnotation")
                            .markFailure()
                            .message("No record found for annotation reference"));
                }
            }
        }
       return recordCount;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private int loadTransactions(
            JournalCache journalCache,
            KafkaJournalLoaderCallback callback) {


        int recordCount = 0;
        for (TransactionReference txref: journalCache.getTransactionRefs()) {
            List<RecordInfo> txRecords = new LinkedList<>();
            List<RecordInfo> txDeleteRecords = new LinkedList<>();
            boolean prepared = false;
            byte[] txData = null;

            for (JournalEntryKey refKey : txref.getEntryReferencesList()) {
                JournalEntry entry = journalCache.getCache().get(refKey);
                recordCount++;

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
                        .addTransactionRef(txref));

                PreparedTransactionInfo pti = new PreparedTransactionInfo(txref.getTxId(), txData);
                pti.getRecords().addAll(txRecords);
                pti.getRecordsToDelete().addAll(txDeleteRecords);
                callback.addPreparedTransaction(pti);
            }
        }
        return recordCount;
    }
}
