/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.persistence.domain.proto.JournalRecordType.COMMIT_TX;
import static io.confluent.amq.persistence.domain.proto.JournalRecordType.PREPARE_TX;
import static io.confluent.amq.persistence.domain.proto.JournalRecordType.ROLLBACK_TX;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.ReconciledMessage;
import io.confluent.amq.persistence.kafka.journal.KafkaRecordInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;

public class KafkaJournalTxHandler {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaJournalTxHandler.class));

  // Track Tx Records
  private final ConcurrentHashMap<Long, TransactionHolder> transactions = new ConcurrentHashMap<>();
  private final String journalTopic;

  public KafkaJournalTxHandler(String journalTopic) {
    this.journalTopic = journalTopic;
  }


  private TransactionHolder upsertTxHolder(JournalRecord record) {

    TransactionHolder txHolder = transactions.computeIfAbsent(
        record.getTxId(), TransactionHolder::new);

    //XA record most likely that has associated metadata
    if (record.getRecordType() == PREPARE_TX) {
      txHolder.extraData = record.getData().toByteArray();
      txHolder.prepared = true;
    }

    return txHolder;
  }

  private List<ReconciledMessage> rollbackTx(
      TransactionHolder txHolder, JournalEntryKey key, JournalRecord record) {

    //add in the rollback record so it also gets tombstoned
    txHolder.recordInfos.add(new KafkaRecordInfo(key, record));

    //tombstone the entire transaction
    return txHolder.recordInfos.stream()
        .map(ki ->
            ReconciledMessage.tombstone(JournalEntryKey.newBuilder(ki.getKafkaKey()).build()))
        .collect(Collectors.toList());
  }

  private List<ReconciledMessage> commitTx(
      TransactionHolder txHolder, JournalEntryKey key, JournalRecord record) {

    List<ReconciledMessage> msgs = new LinkedList<>();
    for (KafkaRecordInfo ki : txHolder.getRecordInfos()) {
      //we issue new records overwriting the old ones

      JournalRecordType updType = convertTxRecordType(ki.getJournalRecord().getRecordType());
      if (ki.getJournalRecord().getRecordType() != updType) {

        //overwrite the existing record key
        JournalEntryKey updKey = JournalEntryKey.newBuilder(ki.getKafkaKey()).build();

        //remove the transaction properties
        JournalEntry updRecord = JournalEntry.newBuilder()
            .setAppendedRecord(JournalRecord.newBuilder(ki.getJournalRecord())
                .setRecordType(updType)
                .clearTxId())
            .build();

        msgs.add(
            ReconciledMessage.forward(updKey, updRecord));

        SLOG.trace(b -> b.name(journalTopic).event("CommitTX")
            .addJournalEntryKey(updKey).addJournalEntry(updRecord));
      } else {
        //tombstone the TX protocol messages
        JournalEntryKey tsKey = JournalEntryKey.newBuilder(ki.getKafkaKey()).build();
        msgs.add(ReconciledMessage.tombstone(tsKey));

        SLOG.trace(b -> {
          b.name(journalTopic)
              .event("Tombstone")
              .addJournalEntryKey(ki.getKafkaKey())
              .addJournalRecord(ki.getJournalRecord());
        });
      }
    }

    //finally tombstone the commit record
    msgs.add(ReconciledMessage.tombstone(key));

    return msgs;
  }

  private JournalRecordType convertTxRecordType(JournalRecordType txType) {
    switch (txType) {
      case ADD_RECORD_TX:
        return JournalRecordType.ADD_RECORD;
      case DELETE_RECORD_TX:
        return JournalRecordType.DELETE_RECORD;
      case ANNOTATE_RECORD_TX:
        return JournalRecordType.ANNOTATE_RECORD;
      default:
        return txType;
    }
  }

  private boolean isTxTerminator(JournalRecord record) {
    return record.getRecordType() == ROLLBACK_TX
        || record.getRecordType() == COMMIT_TX;
  }

  public List<ReconciledMessage> handleTxRecord(JournalEntryKey key, JournalRecord record) {
    List<ReconciledMessage> reconciledMessages = Collections.emptyList();

    TransactionHolder txHolder;
    if (isTxTerminator(record)) {
      txHolder = transactions.get(record.getTxId());
    } else {
      txHolder = upsertTxHolder(record);
    }

    if (txHolder == null) {
      return Collections.singletonList(
          ReconciledMessage.deadletter(
              key,
              JournalEntry.newBuilder().setAppendedRecord(record).build(),
              "TransactionInvalid",
              "Record's indicated transaction is not active/found."));
    }

    switch (record.getRecordType()) {
      case ADD_RECORD_TX:
      case ANNOTATE_RECORD_TX:
        txHolder.recordInfos.add(new KafkaRecordInfo(key, record));
        break;
      case DELETE_RECORD_TX:
        txHolder.recordInfos.add(new KafkaRecordInfo(key, record, true));
        break;
      case ROLLBACK_TX:
        reconciledMessages = rollbackTx(txHolder, key, record);
        transactions.remove(txHolder.transactionID);
        break;
      case COMMIT_TX:
        reconciledMessages = commitTx(txHolder, key, record);
        break;
      default:
        SLOG.warn(b -> b
            .name(journalTopic)
            .event("NotTXRecord")
            .addJournalEntryKey(key)
            .addJournalRecord(record));
        break;
    }

    return reconciledMessages;
  }

  public List<TransactionHolder> getOpenTransactions() {
    return new ArrayList<>(transactions.values());
  }

  public List<PreparedTransactionInfo> preparedTransactions() {
    return transactions.values().stream()
        .filter(th -> th.prepared)
        .map(th -> new PreparedTransactionInfo(th.transactionID, th.extraData))
        .collect(Collectors.toList());
  }

  public static class TransactionHolder {

    private final long transactionID;
    private final List<KafkaRecordInfo> recordInfos = new ArrayList<>();
    private boolean prepared;
    private byte[] extraData;

    TransactionHolder(final long id) {
      transactionID = id;
    }


    public long getTransactionID() {
      return transactionID;
    }

    public List<KafkaRecordInfo> getRecordInfos() {
      return recordInfos;
    }

    public boolean isPrepared() {
      return prepared;
    }

    public byte[] getExtraData() {
      return extraData;
    }
  }

}
