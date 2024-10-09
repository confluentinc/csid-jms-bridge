/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.*;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.LoadInitializer;
import io.confluent.amq.persistence.kafka.journal.JournalStreamTransformer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class TransactionProcessor
    extends JournalStreamTransformer<ValueAndTimestamp<JournalEntry>>
    implements Punctuator {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(TransactionProcessor.class));

  private final long transactionTimeoutMs;
  private final long transactionScanIntervalMs;

  public TransactionProcessor(String journalName, String txStoreName) {
    this(journalName,
        txStoreName,
        Duration.ofMinutes(5).toMillis(),
        Duration.ofMinutes(5).toMillis());
  }

  public TransactionProcessor(
      String journalName, String txStoreName, long transactionTimeoutMs, long scanIntervalMs) {
    super(journalName, txStoreName);
    this.transactionTimeoutMs = transactionTimeoutMs;
    this.transactionScanIntervalMs = scanIntervalMs;
  }

  @Override
  public void init(ProcessorContext context) {
    super.init(context);
    context.schedule(
        Duration.ofMillis(transactionScanIntervalMs), PunctuationType.STREAM_TIME, this);
  }

  @Override
  public void punctuate(long timestamp) {
    SLOG.debug(b -> b
        .name(getJournalName())
        .event("TransactionExpiryScan")
        .markStarted()
        .putTokens("streamTimestamp", Instant.ofEpochMilli(timestamp)));
    List<ValueAndTimestamp<JournalEntry>> txRefs = new LinkedList<>();
    try (KeyValueIterator<JournalEntryKey, ValueAndTimestamp<JournalEntry>> all
        = getStore().all()) {

      while (all.hasNext()) {
        KeyValue<JournalEntryKey, ValueAndTimestamp<JournalEntry>> entry = all.next();
        if (entry.value != null
            && entry.value.value() != null
            && entry.value.value().hasTransactionReference()
            && timestamp - entry.value.timestamp() > transactionTimeoutMs) {

          txRefs.add(entry.value);
        }
      }
    }

    for (ValueAndTimestamp<JournalEntry> valueAndTimestamp: txRefs) {
      TransactionReference txRef =  valueAndTimestamp.value().getTransactionReference();
      boolean isPrepared = txRef.getEntryReferencesList().stream()
          .map(k -> getStore().get(JournalEntryKey.fromRefKey(k)))
          .filter(kv -> kv.value() != null && kv.value().hasAppendedRecord())
          .anyMatch(kv ->
              kv.value().getAppendedRecord().getRecordType() == JournalRecordType.PREPARE_TX);

      if (!isPrepared) {
        SLOG.debug(b -> b
            .name(getJournalName())
            .event("ExpireTransaction")
            .putTokens("transactionId", txRef.getTxId())
            .putTokens("timestamp", Instant.ofEpochMilli(valueAndTimestamp.timestamp())));
        rollback(txRef, timestamp);
      }
    }

    SLOG.debug(b -> b
        .name(getJournalName())
        .event("TransactionExpiryScan")
        .markCompleted());
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public Iterable<KeyValue<JournalEntryKey, JournalEntry>> transform(JournalEntryKey readOnlyKey,
      JournalEntry entry) {

    if (entry != null) {
      if (entry.hasEpochEvent()) {

        JournalEntry readyEvent = JournalEntry.newBuilder(entry)
            .mergeEpochEvent(EpochEvent.newBuilder()
                .setEpochStage(LoadInitializer.EPOCH_STAGE_READY)
                .buildPartial())
            .build();

        SLOG.trace(b -> b
            .name(getJournalName())
            .addJournalEntryKey(readOnlyKey)
            .addJournalEntry(entry)
            .event("ProcessEpochRecord"));

        return Collections.singletonList(KeyValue.pair(readOnlyKey, readyEvent));

        //TX records
      } else if (entry.hasAppendedRecord()
          && KafkaRecordUtils.isTxRecord(entry.getAppendedRecord())) {

        SLOG.trace(b -> b
            .name(getJournalName())
            .addJournalEntryKey(readOnlyKey)
            .addJournalEntry(entry)
            .event("ProcessTransactionRecord"));

        return processTx(readOnlyKey, entry);
      }
    }

    SLOG.trace(b -> b
        .name(getJournalName())
        .addJournalEntryKey(readOnlyKey)
        .addJournalEntry(entry)
        .event("RecordPassThrough"));

    //passthrough
    return Collections.singletonList(KeyValue.pair(readOnlyKey, entry));
  }

  private Iterable<KeyValue<JournalEntryKey, JournalEntry>> processTx(
      JournalEntryKey key, JournalEntry entry) {

    List<KeyValue<JournalEntryKey, JournalEntry>> results = Collections.emptyList();

    long txId = entry.getAppendedRecord().getTxId();
    TransactionReference txReference = findTransactionRef(txId);
    final long timestamp = getContext().timestamp();

    switch (entry.getAppendedRecord().getRecordType()) {
      case COMMIT_TX:
        results = commit(txReference, timestamp);
        break;
      case ROLLBACK_TX:
        results = rollback(txReference, timestamp);
        break;
      case PREPARE_TX:
      case ADD_RECORD_TX:
      case DELETE_RECORD_TX:
      case ANNOTATE_RECORD_TX:
        //aggregate onto current reference
        results = updateTxReferences(key, entry, txReference, timestamp);
        break;
      default:
        SLOG.warn(b -> b
            .name(getJournalName())
            .event("NotTXRecord")
            .addJournalEntryKey(key)
            .addJournalEntry(entry));
        break;

    }

    return results;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> updateTxReferences(
      JournalEntryKey newRecordKey, JournalEntry entry, TransactionReference txReference,
      long timestamp) {

    ValueAndTimestamp<JournalEntry> txRecord = ValueAndTimestamp
        .make(entry, timestamp);

    getStore().put(newRecordKey, txRecord);

    JournalEntryKey txRefKey =
        KafkaRecordUtils.transactionReferenceKeyFromTxId(txReference.getTxId());

    JournalEntry updatedRefEntry = JournalEntry.newBuilder()
        .setTransactionReference(TransactionReference.newBuilder()
            .setTxId(txReference.getTxId())
            .addAllEntryReferences(txReference.getEntryReferencesList())
            .addEntryReferences(newRecordKey.toRefKey()))
        .build();

    ValueAndTimestamp<JournalEntry> valAndTs = ValueAndTimestamp
        .make(updatedRefEntry, timestamp);

    getStore().put(txRefKey, valAndTs);

    return Collections.emptyList();
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private List<KeyValue<JournalEntryKey, JournalEntry>> commit(
      TransactionReference txReference, long timestamp) {

    List<KeyValue<JournalEntryKey, JournalEntry>> txResults = new LinkedList<>();

    for (JournalEntryRefKey key : txReference.getEntryReferencesList()) {
      JournalEntryKey journalEntryKey = JournalEntryKey.fromRefKey(key);
      ValueAndTimestamp<JournalEntry> valAndTs = getStore().get(journalEntryKey);

      if (valAndTs == null || valAndTs.value() == null) {
        SLOG.warn(b -> b
            .name(getJournalName())
            .event("TxRecordNotFound")
            .addJournalEntryKey(journalEntryKey)
            .putTokens("storeName", getStoreName())
            .message("JournalEntry for transaction reference was not found in store!"));
      } else {
        JournalRecord record = valAndTs.value().getAppendedRecord();

        //this should not be possible
        JournalRecordType newRecordType = convertTxRecordType(record.getRecordType());
        if (newRecordType != JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE) {
          JournalEntry newEntry = JournalEntry.newBuilder()
              .setAppendedRecord(JournalRecord.newBuilder(record)
                  .clearTxId()
                  .setRecordType(newRecordType))
              .build();

          JournalEntryKey newRecordKey = KafkaRecordUtils.keyFromEntry(newEntry);
          txResults.add(KeyValue.pair(newRecordKey, newEntry));
        } else {
          //bad ref in TX
          SLOG.warn(b -> b
              .name(getJournalName())
              .event("TxReferenceBad")
              .addJournalEntryKey(journalEntryKey)
              .addJournalRecord(record)
              .putTokens("storeName", getStoreName())
              .message("JournalEntry for transaction reference is not a transaction record!"));

        }
      }
    }
    cleanupTx(txReference, timestamp);
    return txResults;
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

  private List<KeyValue<JournalEntryKey, JournalEntry>> rollback(
      TransactionReference txReference, long timestamp) {

    cleanupTx(txReference, timestamp);

    return Collections.emptyList();
  }

  private void cleanupTx(TransactionReference txReference, long timestamp) {

    //delete all tx records
    for (JournalEntryRefKey key : txReference.getEntryReferencesList()) {
      getStore().delete(JournalEntryKey.fromRefKey(key));
    }

    //delete the TX record and reference itself
    JournalEntryKey txKey = KafkaRecordUtils.transactionKeyFromTxId(txReference.getTxId());
    getStore().delete(txKey);

    JournalEntryKey txRefKey = KafkaRecordUtils
        .transactionReferenceKeyFromTxId(txReference.getTxId());
    getStore().delete(txRefKey);
  }


  private TransactionReference findTransactionRef(Long txId) {
    JournalEntryKey txRefKey = KafkaRecordUtils.transactionReferenceKeyFromTxId(txId);
    ValueAndTimestamp<JournalEntry> txReferenceTs = getStore().get(txRefKey);

    TransactionReference txReference;
    if (txReferenceTs == null) {
      txReference = TransactionReference.newBuilder().setTxId(txId).build();
    } else {
      txReference = txReferenceTs.value().getTransactionReference();
    }

    return txReference;
  }
}
