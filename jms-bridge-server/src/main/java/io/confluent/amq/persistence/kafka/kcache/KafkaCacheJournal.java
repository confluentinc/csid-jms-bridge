package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.logging.StructuredLogger;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.*;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaCacheJournal implements Journal {

  private static final StructuredLogger SLOG =
      StructuredLogger.with(b -> b.loggerClass(KafkaCacheJournal.class));

  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;

  private final String journalName;

  @Override
  public void appendAddRecord(long l, byte b, byte[] bytes, boolean b1) throws Exception {}

  @Override
  public void appendAddRecord(long l, byte b, Persister persister, Object o, boolean b1)
      throws Exception {}

  @Override
  public void appendAddRecord(
      long l, byte b, Persister persister, Object o, boolean b1, IOCompletion ioCompletion)
      throws Exception {}

  @Override
  public void appendUpdateRecord(long l, byte b, byte[] bytes, boolean b1) throws Exception {}

  @Override
  public void tryAppendUpdateRecord(
      long l,
      byte b,
      byte[] bytes,
      JournalUpdateCallback journalUpdateCallback,
      boolean b1,
      boolean b2)
      throws Exception {}

  @Override
  public void appendUpdateRecord(long l, byte b, Persister persister, Object o, boolean b1)
      throws Exception {}

  @Override
  public void tryAppendUpdateRecord(
      long l,
      byte b,
      Persister persister,
      Object o,
      JournalUpdateCallback journalUpdateCallback,
      boolean b1,
      boolean b2)
      throws Exception {}

  @Override
  public void appendUpdateRecord(
      long l, byte b, Persister persister, Object o, boolean b1, IOCompletion ioCompletion)
      throws Exception {}

  @Override
  public void tryAppendUpdateRecord(
      long l,
      byte b,
      Persister persister,
      Object o,
      boolean b1,
      boolean b2,
      JournalUpdateCallback journalUpdateCallback,
      IOCompletion ioCompletion)
      throws Exception {}

  @Override
  public void appendDeleteRecord(long l, boolean b) throws Exception {}

  @Override
  public void tryAppendDeleteRecord(long l, JournalUpdateCallback journalUpdateCallback, boolean b)
      throws Exception {}

  @Override
  public void appendDeleteRecord(long l, boolean b, IOCompletion ioCompletion) throws Exception {}

  @Override
  public void tryAppendDeleteRecord(
      long l, boolean b, JournalUpdateCallback journalUpdateCallback, IOCompletion ioCompletion)
      throws Exception {}

  @Override
  public void appendAddRecordTransactional(long l, long l1, byte b, byte[] bytes)
      throws Exception {}

  @Override
  public void appendAddRecordTransactional(long l, long l1, byte b, Persister persister, Object o)
      throws Exception {}

  @Override
  public void appendUpdateRecordTransactional(long l, long l1, byte b, byte[] bytes)
      throws Exception {}

  @Override
  public void appendUpdateRecordTransactional(
      long l, long l1, byte b, Persister persister, Object o) throws Exception {}

  @Override
  public void appendDeleteRecordTransactional(long l, long l1, byte[] bytes) throws Exception {}

  @Override
  public void appendDeleteRecordTransactional(long l, long l1, EncodingSupport encodingSupport)
      throws Exception {}

  @Override
  public void appendDeleteRecordTransactional(long l, long l1) throws Exception {}

  @Override
  public void appendCommitRecord(long l, boolean b) throws Exception {}

  @Override
  public void appendCommitRecord(long l, boolean b, IOCompletion ioCompletion) throws Exception {}

  @Override
  public void appendCommitRecord(long l, boolean b, IOCompletion ioCompletion, boolean b1)
      throws Exception {}

  @Override
  public void appendPrepareRecord(long l, EncodingSupport encodingSupport, boolean b)
      throws Exception {}

  @Override
  public void appendPrepareRecord(
      long l, EncodingSupport encodingSupport, boolean b, IOCompletion ioCompletion)
      throws Exception {}

  @Override
  public void appendPrepareRecord(long l, byte[] bytes, boolean b) throws Exception {}

  @Override
  public void appendRollbackRecord(long l, boolean b) throws Exception {}

  @Override
  public void appendRollbackRecord(long l, boolean b, IOCompletion ioCompletion) throws Exception {}

  @Override
  public JournalLoadInformation load(LoaderCallback loaderCallback) throws Exception {
    return null;
  }

  @Override
  public JournalLoadInformation loadInternalOnly() throws Exception {
    return null;
  }

  @Override
  public JournalLoadInformation loadSyncOnly(JournalState journalState) throws Exception {
    return null;
  }

  @Override
  public void lineUpContext(IOCompletion ioCompletion) {}

  @Override
  public JournalLoadInformation load(
      List<RecordInfo> list,
      List<PreparedTransactionInfo> list1,
      TransactionFailureCallback transactionFailureCallback,
      boolean b)
      throws Exception {
    return null;
  }

  @Override
  public JournalLoadInformation load(
      SparseArrayLinkedList<RecordInfo> sparseArrayLinkedList,
      List<PreparedTransactionInfo> list,
      TransactionFailureCallback transactionFailureCallback,
      boolean b)
      throws Exception {
    return null;
  }

  @Override
  public int getAlignment() throws Exception {
    SLOG.debug(b -> b.name(journalName).event("UnimplementedMethodCall").message("getAlignment"));

    // required for replication, not needed so return 0
    return 0;
  }

  @Override
  public int getNumberOfRecords() {
    return 0;
  }

  @Override
  public int getUserVersion() {
    SLOG.debug(b -> b.name(journalName).event("UnimplementedMethodCall").message("getUserVersion"));
    return 0;
  }

  @Override
  public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception {
    SLOG.debug(
        b ->
            b.name(journalName)
                .event("UnimplementedMethodCall")
                .message("createFilesForBackupSync"));

    return Collections.emptyMap();
  }

  @Override
  public void synchronizationLock() {
    SLOG.debug(
        b -> b.name(journalName).event("UnimplementedMethodCall").message("synchronizationLock"));
    // do nothing
  }

  @Override
  public void synchronizationUnlock() {
    SLOG.debug(
        b -> b.name(journalName).event("UnimplementedMethodCall").message("synchronizationUnlock"));
    // do nothing
  }

  @Override
  public void forceMoveNextFile() throws Exception {
    SLOG.debug(
        b -> b.name(journalName).event("UnimplementedMethodCall").message("forceMoveNextFile"));
    // do nothing
  }

  @Override
  public JournalFile[] getDataFiles() {
    SLOG.debug(b -> b.name(journalName).event("UnimplementedMethodCall").message("getDataFiles"));
    return new JournalFile[0];
  }

  @Override
  public SequentialFileFactory getFileFactory() {
    SLOG.debug(b -> b.name(journalName).event("UnimplementedMethodCall").message("getFileFactory"));
    return null;
  }

  @Override
  public int getFileSize() {
    return 0;
  }

  @Override
  public void scheduleCompactAndBlock(int timeout) throws Exception {
    SLOG.debug(
        b ->
            b.name(journalName)
                .event("UnimplementedMethodCall")
                .message("scheduleCompactAndBlock"));
    // do nothing
  }

  @Override
  public void replicationSyncPreserveOldFiles() {
    SLOG.debug(
        b ->
            b.name(journalName)
                .event("UnimplementedMethodCall")
                .message("scheduleCompactAndBlock"));
    // do nothing
  }

  @Override
  public void replicationSyncFinished() {
    SLOG.debug(
        b ->
            b.name(journalName)
                .event("UnimplementedMethodCall")
                .message("replicationSyncFinished"));
    // do nothing
  }

  @Override
  public void flush() throws Exception {
    SLOG.debug(b -> b.name(journalName).event("UnimplementedMethodCall").message("flush"));
    // do nothing
  }

  /**
   * An event is data recorded on the journal, but it won't have any weight or deletes. It's always
   * ready to be removed. It is useful on recovery data while in use with backup history journal.
   */
  @Override
  public void appendAddEvent(
      long l, byte b, Persister persister, Object o, boolean b1, IOCompletion ioCompletion)
      throws Exception {
    // no-op
  }

  @Override
  public void setRemoveExtraFilesOnLoad(boolean b) {
    // no-op
  }

  @Override
  public boolean isRemoveExtraFilesOnLoad() {
    return false;
  }

  @Override
  public long getMaxRecordSize() {
    return MAX_RECORD_SIZE;
  }

  @Override
  public void start() throws Exception {}

  @Override
  public void stop() throws Exception {}

  @Override
  public boolean isStarted() {
    return false;
  }
}
