/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;

public interface KafkaJournalLoaderCallback extends LoaderCallback {

  static KafkaJournalLoaderCallback from(final List<RecordInfo> committedRecords,
      final List<PreparedTransactionInfo> preparedTransactions,
      final TransactionFailureCallback failureCallback,
      final boolean fixBadTX) {

    return wrap(
        new DefaultCallback(committedRecords, preparedTransactions, failureCallback, fixBadTX));
  }

  static KafkaJournalLoaderCallback wrap(LoaderCallback delegate) {
    if (delegate instanceof KafkaJournalLoaderCallback) {
      return (KafkaJournalLoaderCallback) delegate;
    }
    return new WrappedCallback(delegate);
  }

  /**
   * Communicates that loading of the journal has completed.
   *
   * @param recordCount the number of records loaded from the journal
   */
  void loadComplete(int recordCount);

  /**
   * Waits for loading to complete to return the load information. If loading has already completed
   * then it returns immediately.
   *
   * @return metadata on the loading of the journal
   */
  JournalLoadInformation getLoadInfo();


  class WrappedCallback implements KafkaJournalLoaderCallback {

    private final LoaderCallback delegate;
    private final CompletableFuture<JournalLoadInformation> loadComplete;
    private final AtomicLong maxId = new AtomicLong(-1);

    private WrappedCallback(LoaderCallback delegate) {
      this.delegate = delegate;
      this.loadComplete = new CompletableFuture<>();
    }

    private void checkMaxId(final long id) {
      maxId.getAndUpdate(prevId -> Math.max(prevId, id));
    }

    @Override
    public void addPreparedTransaction(
        PreparedTransactionInfo preparedTransaction) {
      delegate.addPreparedTransaction(preparedTransaction);
    }

    @Override
    public void addRecord(RecordInfo info) {
      delegate.addRecord(info);
      checkMaxId(info.id);
    }

    @Override
    public void deleteRecord(long id) {
      delegate.deleteRecord(id);
    }

    @Override
    public void updateRecord(RecordInfo info) {
      delegate.updateRecord(info);
    }

    @Override
    public void failedTransaction(long transactionID,
        List<RecordInfo> records,
        List<RecordInfo> recordsToDelete) {
      delegate.failedTransaction(transactionID, records, recordsToDelete);
    }

    @Override
    public void loadComplete(int recordCount) {
      JournalLoadInformation jli = new JournalLoadInformation(recordCount, maxId.get());
      this.loadComplete.complete(jli);
    }

    @Override
    public JournalLoadInformation getLoadInfo() {
      try {
        return loadComplete.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


  class DefaultCallback implements LoaderCallback {

    private final List<PreparedTransactionInfo> preparedTransactions;

    private final TransactionFailureCallback failureCallback;

    /* We keep track of list entries for each ID.
       This preserves order and allows multiple record insertions with the
       same ID.  We use this for deleting records */
    private final Map<Long, List<Integer>> deleteReferences = new HashMap<>();

    private final List<RecordInfo> committedRecords;

    private final boolean fixBadTx;

    private DefaultCallback(final List<RecordInfo> committedRecords,
        final List<PreparedTransactionInfo> preparedTransactions,
        final TransactionFailureCallback failureCallback,
        final boolean fixBadTX) {

      this.fixBadTx = fixBadTX;
      this.committedRecords = committedRecords;
      this.preparedTransactions = preparedTransactions;
      this.failureCallback = failureCallback;
    }

    @Override
    public void addPreparedTransaction(final PreparedTransactionInfo preparedTransaction) {
      preparedTransactions.add(preparedTransaction);
    }

    @Override
    public synchronized void addRecord(final RecordInfo info) {
      int index = committedRecords.size();
      committedRecords.add(index, info);

      ArrayList<Integer> indexes = new ArrayList<>();
      indexes.add(index);

      deleteReferences.put(info.id, indexes);
    }

    @Override
    public synchronized void updateRecord(final RecordInfo info) {
      int index = committedRecords.size();
      committedRecords.add(index, info);
    }

    @Override
    public synchronized void deleteRecord(final long id) {
      for (int i : deleteReferences.get(id)) {
        committedRecords.remove(i);
      }
    }

    @Override
    public void failedTransaction(final long transactionID,
        final List<RecordInfo> records,
        final List<RecordInfo> recordsToDelete) {
      if (failureCallback != null) {
        failureCallback.failedTransaction(transactionID, records, recordsToDelete);
      }
    }
  }
}

