/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.JournalStreamTransformer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class TransactionProcessor extends JournalStreamTransformer {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(TransactionProcessor.class));

  public TransactionProcessor(String journalName, String storeName) {
    super(journalName, storeName);
  }

  @Override
  public Iterable<KeyValue<JournalEntryKey, JournalEntry>> transform(JournalEntryKey readOnlyKey,
      JournalEntry entry) {

    if (entry == null
        || !entry.hasAppendedRecord()
        || !KafkaRecordUtils.isTxRecord(entry.getAppendedRecord())) {

      //passthrough
      return Collections.singletonList(KeyValue.pair(readOnlyKey, entry));
    }

    List<KeyValue<JournalEntryKey, JournalEntry>> results = Collections.emptyList();

    long txId = entry.getAppendedRecord().getTxId();
    Pair<TransactionReference, Long> txReferencePair = findTransactionRef(txId);
    TransactionReference txReference = txReferencePair.getLeft();
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
        results = updateTxReferences(readOnlyKey, txReference, timestamp);
        break;
      default:
        SLOG.warn(b -> b
            .name(getJournalName())
            .event("NotTXRecord")
            .addJournalEntryKey(readOnlyKey)
            .addJournalEntry(entry));
        break;

    }


    if (SLOG.logger().isDebugEnabled()) {
      results.forEach(kv ->
          SLOG.debug(b -> {
            b.name(getJournalName())
                .putTokens("timestamp", timestamp);
            if (kv.value != null) {
              b.event("ENTRY")
                  .addJournalEntryKey(kv.key)
                  .addJournalEntry(kv.value);
            } else {
              b.event("TOMBSTONE")
                  .addJournalEntryKey(kv.key);
            }
          }));
    }

    return results;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> updateTxReferences(
      JournalEntryKey newRecordKey, TransactionReference txReference, long timestamp) {

    JournalEntryKey txRefKey =
        KafkaRecordUtils.transactionReferenceKeyFromTxId(txReference.getTxId());

    JournalEntry updatedRefEntry = JournalEntry.newBuilder()
        .setTransactionReference(TransactionReference.newBuilder()
            .setTxId(txReference.getTxId())
            .addAllEntryReferences(txReference.getEntryReferencesList())
            .addEntryReferences(JournalEntryKey.newBuilder(newRecordKey)))
        .build();

    ValueAndTimestamp<JournalEntry> valAndTs = ValueAndTimestamp
        .make(updatedRefEntry, timestamp);

    getStore().put(txRefKey, valAndTs);

    return Collections.singletonList(KeyValue.pair(txRefKey, updatedRefEntry));
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private List<KeyValue<JournalEntryKey, JournalEntry>> commit(
      TransactionReference txReference, long timestamp) {

    List<KeyValue<JournalEntryKey, JournalEntry>> txResults = new LinkedList<>();

    for (JournalEntryKey key : txReference.getEntryReferencesList()) {
      ValueAndTimestamp<JournalEntry> valAndTs = getStore().get(key);

      if (valAndTs == null || valAndTs.value() == null) {
        SLOG.warn(b -> b
            .name(getJournalName())
            .event("TxRecordNotFound")
            .addJournalEntryKey(key)
            .putTokens("storeName", getStoreName())
            .message("JournalEntry for transaction reference was not found in store!"));
      } else {
        JournalRecord record = valAndTs.value().getAppendedRecord();
        JournalRecordType newRecordType = JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE;

        switch (record.getRecordType()) {
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
            //do nothing, shouldn't happen
            break;
          default:
            //bad ref in TX
            SLOG.warn(b -> b
                .name(getJournalName())
                .event("TxReferenceBad")
                .addJournalEntryKey(key)
                .addJournalRecord(record)
                .putTokens("storeName", getStoreName())
                .message("JournalEntry for transaction reference is not a transaction record!"));
            break;
        }

        if (newRecordType != JournalRecordType.UNKNOWN_JOURNAL_RECORD_TYPE) {
          JournalEntry newEntry = JournalEntry.newBuilder()
              .setAppendedRecord(JournalRecord.newBuilder(record)
                  .clearTxId()
                  .setRecordType(newRecordType))
              .build();

          JournalEntryKey newRecordKey = KafkaRecordUtils.keyFromEntry(newEntry);
          txResults.add(KeyValue.pair(newRecordKey, newEntry));
          getStore().put(newRecordKey, ValueAndTimestamp.make(newEntry, timestamp));
        }
      }
    }

    //delete transaction records
    List<KeyValue<JournalEntryKey, JournalEntry>> finalResults = new LinkedList<>();
    finalResults.addAll(tombstones(txReference, timestamp));
    finalResults.addAll(txResults);
    return finalResults;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> rollback(
      TransactionReference txReference, long timestamp) {

    return tombstones(txReference, timestamp);
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> tombstones(
      TransactionReference txReference, long timestamp) {

    List<KeyValue<JournalEntryKey, JournalEntry>> tsList = new LinkedList<>();
    //delete all tx records
    for (JournalEntryKey key : txReference.getEntryReferencesList()) {
      tsList.add(KeyValue.pair(key, null));
      getStore().delete(key);
    }

    //delete the TX record and reference itself
    JournalEntryKey txKey = KafkaRecordUtils.transactionKeyFromTxId(txReference.getTxId());
    tsList.add(KeyValue.pair(txKey, null));
    getStore().delete(txKey);

    JournalEntryKey txRefKey = KafkaRecordUtils
        .transactionReferenceKeyFromTxId(txReference.getTxId());
    tsList.add(KeyValue.pair(txRefKey, null));
    getStore().delete(txRefKey);

    return tsList;
  }


  private Pair<TransactionReference, Long> findTransactionRef(Long txId) {
    JournalEntryKey txRefKey = KafkaRecordUtils.transactionReferenceKeyFromTxId(txId);
    ValueAndTimestamp<JournalEntry> txReferenceTs = getStore().get(txRefKey);

    TransactionReference txReference;
    long timestamp;
    if (txReferenceTs == null) {

      txReference = TransactionReference.newBuilder().setTxId(txId).build();
      timestamp = getContext().timestamp();

    } else {
      txReference = txReferenceTs.value().getTransactionReference();
      timestamp = Math.max(txReferenceTs.timestamp(), getContext().timestamp());
    }

    return Pair.of(txReference, timestamp);
  }
}
