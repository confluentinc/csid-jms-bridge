package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.*;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.kcache.KafkaCache;
import lombok.SneakyThrows;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * This class is responsible for reading a WAL topic and resolving it against the state cache.
 */
public class WalResolver {
    public static final Duration DEFAULT_TX_TTL = Duration.ofMinutes(5L);
    private static final StructuredLogger SLOG = StructuredLogger
            .with(b -> b.loggerClass(WalResolver.class));

    private final ConcurrentHashMap<TimestampedJournalKey, TransactionReference> txCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TimestampedJournalKey, AnnotationReference> annCache = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<JournalKeyValue> walSource = new LinkedBlockingQueue<>();
    private final Supplier<KafkaCache<JournalEntryKey, JournalEntry>> cacheSupplier;

    private final Duration txTTL;

    private String journalName;
    private volatile boolean isProcessing = false;
    private volatile boolean isReading = false;

    private volatile ForkJoinTask<?> processingThread;

    public WalResolver(String journalName,
                       Duration txTTL,
                       Supplier<KafkaCache<JournalEntryKey, JournalEntry>> cacheSupplier) {

        this.cacheSupplier = cacheSupplier;
        this.journalName = journalName;
        this.txTTL = txTTL;
    }
    public WalResolver(String journalName,
                       Supplier<KafkaCache<JournalEntryKey, JournalEntry>> cacheSupplier) {
        this(journalName, DEFAULT_TX_TTL, cacheSupplier);
    }

    /**
     * This will begin the resolution of the WAL in read only mode. Internal caches will be maintained to properly
     * perform resolution when processing is started.
     * A prerequisite of this method is that the KafkaCache is available via the suppliers.
     */
    public void startReadOnlyProcessing() {
        startWorkThread();
    }

    /**
     * Begins processing and writing the results to the WAL.
     * This can be invoked after {@link #startReadOnlyProcessing()} or on its own.
     */
    public synchronized void startProcessing() {
        if (!isProcessing) {
            isProcessing = true;
            startWorkThread();
        }
    }

    /**
     * Stops all processing including read-only.
     */
    public synchronized void stop() {
        if (isProcessing) {
            isReading = false;
            isProcessing = false;
            processingThread.join();
        }
    }

    private synchronized void startWorkThread() {
        if (getCache() == null) {
            throw new IllegalStateException("Cannot start processing without the cache being available.");
        }

        this.isReading = true;
        if (this.processingThread == null) {
            this.processingThread = ForkJoinPool.commonPool().submit(this::processLog);
        }
    }

    protected KafkaCache<JournalEntryKey, JournalEntry> getCache() {
        return cacheSupplier.get();
    }

    @SneakyThrows
    protected void processLog() {
        while (isReading) {
            JournalKeyValue keyVal = walSource.poll(100, TimeUnit.MILLISECONDS);
            if (keyVal != null) {
                JournalEntry entry = keyVal.getValue();
                JournalEntryKey key = keyVal.getKey();

                if (KafkaRecordUtils.isTxRecord(entry.getAppendedRecord())) {
                    handleTxRecord(keyVal);
                } else if (JournalRecordType.DELETE_RECORD.equals(entry.getAppendedRecord().getRecordType())) {
                    handleDelete(key);
                } else if (JournalRecordType.ANNOTATE_RECORD.equals(entry.getAppendedRecord().getRecordType())) {
                    handleAnnotation(keyVal);
                } else {
                    SLOG.warn(b -> b
                            .addJournalEntryKey(key)
                            .addJournalEntry(entry)
                            .event("UnidentifiedRecord"));
                }
            }
        }
    }

    private void handleAnnotation(JournalKeyValue keyValue) {

        JournalEntryKey annRefKey = KafkaRecordUtils.annotationsKeyFromRecordKey(keyValue.getKey());
        AnnotationReference annRef = annCache.get(annRefKey);
        if (annRef != null) {
            //need to update the annotation reference
            AnnotationReference updatedAnnRef = AnnotationReference
                    .newBuilder()
                    .setMessageId(annRef.getMessageId())
                    .addAllEntryReferences(keyValue.getValue().getAnnotationReference().getEntryReferencesList())
                    .build();

            annCache.put(new TimestampedJournalKey(annRefKey, keyValue.getTimestamp()), updatedAnnRef);
        } else {
            //first annotation
            AnnotationReference firstAnnRef = AnnotationReference
                    .newBuilder()
                    .setMessageId(keyValue.getKey().getMessageId())
                    .addEntryReferences(keyValue.getKey())
                    .build();
            annCache.put(new TimestampedJournalKey(annRefKey, keyValue.getTimestamp()), firstAnnRef);
        }
    }

    private void handleDelete(JournalEntryKey key) {

        JournalEntryKey annRefKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

        //delete all related annotations and the key
        AnnotationReference annRef = annCache.remove(annRefKey);

        if (isProcessing) {
            if (annRef != null) {
                annRef.getEntryReferencesList().forEach(getCache()::remove);
            }

            //delete main record
            getCache().remove(key);
        }
    }

    protected void handleTxRecord(JournalKeyValue keyValue) {
        //TODO: handle timedout transactions
        long txId = keyValue.getValue().getAppendedRecord().getTxId();
        TimestampedJournalKey txKey = new TimestampedJournalKey(
                KafkaRecordUtils.transactionReferenceKeyFromTxId(txId), keyValue.getTimestamp());

        TransactionReference txReference = txCache.computeIfAbsent(txKey, id -> TransactionReference
                        .newBuilder()
                        .setTxId(txId)
                        .build());

        switch (keyValue.getValue().getAppendedRecord().getRecordType()) {
            case COMMIT_TX:
                //tombstones the commit record
                commit(txReference);
                txCache.remove(txKey);
                break;
            case ROLLBACK_TX:
                //tombstones all tx records
                rollback(txReference);
                txCache.remove(txKey);
                break;
            case PREPARE_TX:
            case ADD_RECORD_TX:
            case DELETE_RECORD_TX:
            case ANNOTATE_RECORD_TX:
                //adds to the transaction reference cache
                updateTxReferences(txKey, keyValue.getValue(), txReference);
                break;
            default:
                SLOG.warn(b -> b
                        .name(journalName)
                        .event("NotTXRecord")
                        .addJournalEntryKey(keyValue.getKey())
                        .addJournalEntry(keyValue.getValue()));
                break;

        }
    }

    //TODO: clean up the cache
    private void reapExpiredTransactions() {
        //scan the cache for expired TXs
        txCache.forEach((k, v) -> {
            long ageMs = System.currentTimeMillis() - k.getTimestamp().getTime();
            if(txTTL.minus(ageMs, ChronoUnit.MILLIS).isNegative()) {
                //reap the transaction
                //this may happen multiple times against a single TX as additional TX messages flow in, it's OK.
                cleanupTx(v);
            }
        });
    }


    public void submitEntry(JournalEntryKey key, JournalEntry value, Date recordTime) throws InterruptedException {
        walSource.put(new JournalKeyValue(key, value, recordTime));
    }

    private void updateTxReferences(
            TimestampedJournalKey newRecordKey, JournalEntry entry, TransactionReference txReference) {

        if (isProcessing) {
            getCache().put(newRecordKey.getEntry(), entry);
        }

        TransactionReference updatedRefEntry = TransactionReference.newBuilder()
                .setTxId(txReference.getTxId())
                .addAllEntryReferences(txReference.getEntryReferencesList())
                .addEntryReferences(JournalEntryKey.newBuilder(newRecordKey.getEntry()))
                .build();


        txCache.put(newRecordKey, updatedRefEntry);

    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void commit(TransactionReference txReference) {
        if (!isProcessing) {
            return;
        }

        for (JournalEntryKey key : txReference.getEntryReferencesList()) {
            JournalEntry value = getCache().get(key);

            if (value == null) {
                SLOG.warn(b -> b
                        .name(journalName)
                        .event("TxRecordNotFound")
                        .addJournalEntryKey(key)
                        .message("JournalEntry for transaction reference was not found in store!"));
            } else {
                JournalRecord record = value.getAppendedRecord();

                //this should not be possible
                JournalRecordType newRecordType = convertTxRecordType(record.getRecordType());
                if (newRecordType != JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE) {
                    JournalEntry newEntry = JournalEntry.newBuilder()
                            .setAppendedRecord(JournalRecord.newBuilder(record)
                                    .clearTxId()
                                    .setRecordType(newRecordType))
                            .build();

                    JournalEntryKey newRecordKey = KafkaRecordUtils.keyFromEntry(newEntry);
                    getCache().put(newRecordKey, newEntry);
                } else {
                    //bad ref in TX
                    SLOG.warn(b -> b
                            .name(journalName)
                            .event("TxReferenceBad")
                            .addJournalEntryKey(key)
                            .addJournalRecord(record)
                            .message("JournalEntry for transaction reference is not a transaction record!"));

                }
            }
        }
        cleanupTx(txReference);
    }

    private JournalRecordType convertTxRecordType(JournalRecordType txRecordType) {
        JournalRecordType newRecordType;

        switch (txRecordType) {
            case ADD_RECORD_TX:
                newRecordType = JournalRecordType.ADD_RECORD;
                break;
            case ANNOTATE_RECORD_TX:
                newRecordType = JournalRecordType.ANNOTATE_RECORD;
                break;
            case DELETE_RECORD_TX:
                newRecordType = JournalRecordType.DELETE_RECORD;
                break;
            case PREPARE_TX:
            case COMMIT_TX:
            case ROLLBACK_TX:
            default:
                newRecordType = JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE;
                break;
        }

        return newRecordType;
    }

    private void rollback(
            TransactionReference txReference) {

        if (isProcessing) {
            cleanupTx(txReference);
        }

    }

    private void cleanupTx(TransactionReference txReference) {

        //delete all tx records
        for (JournalEntryKey key : txReference.getEntryReferencesList()) {
            getCache().remove(key);
        }

        //delete the TX record and reference itself
        JournalEntryKey txKey = KafkaRecordUtils.transactionKeyFromTxId(txReference.getTxId());
        getCache().remove(txKey);

        JournalEntryKey txRefKey = KafkaRecordUtils
                .transactionReferenceKeyFromTxId(txReference.getTxId());
        getCache().remove(txRefKey);
    }
}
