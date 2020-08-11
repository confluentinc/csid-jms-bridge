/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType.COMMIT_RECORD;
import static io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType.PREPARE_RECORD;
import static io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType.ROLLBACK_RECORD;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.persistence.kafka.ReconciledMessage;
import io.confluent.amq.persistence.kafka.journal.KafkaRecordInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJournalTxHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJournalTxHandler.class);


  // Track Tx Records
  private final Map<Long, TransactionHolder> transactions = new ConcurrentHashMap<>();
  private final String journalTopic;

  public KafkaJournalTxHandler(String journalTopic) {
    this.journalTopic = journalTopic;
  }


  private TransactionHolder upsertTxHolder(byte[] key, JournalRecord record) {

    TransactionHolder txHolder = transactions.computeIfAbsent(
        record.getTxId(), TransactionHolder::new);

    if (record.getRecordType() == PREPARE_RECORD) {
      txHolder.extraData = record.getData().toByteArray();
      txHolder.prepared = true;
    }

    return txHolder;
  }

  private List<ReconciledMessage<?>> rollbackTx(
      TransactionHolder txHolder, byte[] key, JournalRecord record) {

    txHolder.recordInfos.add(new KafkaRecordInfo(key, record));

    return txHolder.recordInfos.stream()
        .map(ki -> ReconciledMessage.tombstone(journalTopic, ki.getKafkaKey()))
        .collect(Collectors.toList());
  }

  private List<ReconciledMessage<?>> commitTx(
      TransactionHolder txHolder, byte[] key, JournalRecord record) {

    //we don't need the commit record, so we don't add it
    return txHolder.recordInfos.stream()
        .flatMap(ki -> {
          List<ReconciledMessage<?>> msgs = new LinkedList<>();
          JournalRecordType updType = convertTxRecordType(ki.getJournalRecord().getRecordType());

          //Indicates that we want to publish back this part of the transaction
          if (ki.getJournalRecord().getRecordType() != updType) {
            JournalRecord updRecord = JournalRecord.newBuilder(ki.getJournalRecord())
                .setRecordType(updType)
                .clearTxId()
                .clearTxData()
                .clearTxRecordCount()
                .build();

            JournalRecordKey updKey = JournalRecordKey.newBuilder()
                .setId(updRecord.getId())
                .build();

            msgs.add(
                ReconciledMessage.forward(journalTopic, updKey.toByteArray(), updRecord));

            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Committing TX({} #{}) Record: "
                  + "key: tx-{}_id-{}, RecordType: '{}', UserRecordType: '{}'",
                  journalTopic,
                  txHolder.transactionID,
                  updKey.getTxId(),
                  updKey.getId(),
                  updRecord.getRecordType(),
                  updRecord.getUserRecordType());
            }

          }

          //delete the tx meta records, key ==  key
          msgs.add(ReconciledMessage.tombstone(journalTopic, ki.getKafkaKey()));

          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Tombstoning TX({} #{}) Record: "
                    + "key: tx-{}_id-{}, RecordType: '{}', UserRecordType: '{}'",
                journalTopic,
                txHolder.transactionID,
                ki.getJournalRecord().getTxId(),
                ki.getJournalRecord().getId(),
                ki.getJournalRecord().getRecordType(),
                ki.getJournalRecord().getUserRecordType());
          }

          return msgs.stream();

        })
        .collect(Collectors.toList());
  }

  private JournalRecordType convertTxRecordType(JournalRecordType txType) {
    switch (txType) {
      case ADD_RECORD_TX:
        return JournalRecordType.ADD_RECORD;
      case DELETE_RECORD_TX:
        return JournalRecordType.DELETE_RECORD;
      case UPDATE_RECORD_TX:
        return JournalRecordType.UPDATE_RECORD;
      default:
        return txType;
    }
  }

  private boolean isTxTerminator(JournalRecord record) {
    return record.getRecordType() == ROLLBACK_RECORD
        || record.getRecordType() == COMMIT_RECORD;
  }

  public List<ReconciledMessage<?>> handleTxRecord(byte[] key, JournalRecord record) {
    List<ReconciledMessage<?>> reconciledMessages = Collections.emptyList();

    TransactionHolder txHolder;
    if (isTxTerminator(record)) {
      txHolder = transactions.get(record.getTxId());
    } else {
      txHolder = upsertTxHolder(key, record);

      if (txHolder == null) {
        //todo: what to do here, commit/rollback unknown TX
        return reconciledMessages;
      }
    }

    switch (record.getRecordType()) {
      case ADD_RECORD_TX:
      case UPDATE_RECORD_TX:
        txHolder.recordInfos.add(new KafkaRecordInfo(key, record));
        break;
      case DELETE_RECORD_TX:
        txHolder.recordInfos.add(new KafkaRecordInfo(key, record, true));
        break;
      case ROLLBACK_RECORD:
        reconciledMessages = rollbackTx(txHolder, key, record);
        transactions.remove(txHolder.transactionID);
        break;
      case COMMIT_RECORD:
        reconciledMessages = commitTx(txHolder, key, record);
        break;
      default:
        LOGGER.warn("Non-Transaction record was sent to the transaction handler.");
        break;
    }

    return reconciledMessages;
  }

  public List<PreparedTransactionInfo> preparedTransactions() {
    return transactions.values().stream()
        .filter(th -> th.prepared)
        .map(th -> new PreparedTransactionInfo(th.transactionID, th.extraData))
        .collect(Collectors.toList());
  }

  static final class TransactionHolder {

    TransactionHolder(final long id) {
      transactionID = id;
    }

    public final long transactionID;

    final List<KafkaRecordInfo> recordInfos = new ArrayList<>();

    public boolean prepared;

    byte[] extraData;

  }

}
