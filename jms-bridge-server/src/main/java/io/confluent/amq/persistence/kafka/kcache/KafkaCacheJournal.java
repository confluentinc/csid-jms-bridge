package io.confluent.amq.persistence.kafka.kcache;

import com.google.protobuf.ByteString;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.KafkaJournalRecord;
import io.kcache.KafkaCache;
import io.kcache.exceptions.CacheException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.*;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A KafkaCacheJournal is an implementation of an artemis journal that uses Kafka as a cache. To
 * help persist state.
 */
public class KafkaCacheJournal implements Journal {

  private static final StructuredLogger SLOG =
      StructuredLogger.with(b -> b.loggerClass(KafkaCacheJournal.class));
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCacheJournal.class);
  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;

  private final AtomicBoolean failed = new AtomicBoolean(false);

  private final String journalName;
  private IOCriticalErrorListener criticalIOErrorListener;
  private KafkaCache<JournalEntryKey, JournalEntry> journalCache;

  private volatile JournalState state;

  public KafkaCacheJournal(
      String journalName,
      KafkaCache<JournalEntryKey, JournalEntry> journalCache,
      IOCriticalErrorListener criticalIOErrorListener) {
    this.journalName = journalName;
    this.criticalIOErrorListener = criticalIOErrorListener;
    this.journalCache = journalCache;
  }

  private byte[] decode(Persister<Object> persister, Object obj) {
    int size = persister.getEncodeSize(obj);

    ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(size);
    persister.encode(encodedBuffer, obj);

    byte[] buffer = new byte[size];
    encodedBuffer.readBytes(buffer);

    return buffer;
  }

  private byte[] decode(EncodingSupport obj) {
    int size = obj.getEncodeSize();

    ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(size);
    obj.encode(encodedBuffer);

    byte[] buffer = new byte[size];
    encodedBuffer.readBytes(buffer);
    return buffer;
  }

  protected void handleException(Throwable e) {
    LOGGER.warn(e.getMessage(), e);
    failed.set(true);
    criticalIOErrorListener.onIOException(e, "Failed to write to Kafka", null);
  }

  private void checkStatus(IOCompletion callback) throws Exception {
    if (!isReady()) {
      if (callback != null) {
        callback.onError(
            ActiveMQExceptionType.ILLEGAL_STATE.getCode(), "Kafka Cache Journal is not loaded");
      }
      throw new ActiveMQShutdownException("Kafka Cache Journal is not ready");
    }

    if (failed.get()) {
      if (callback != null) {
        callback.onError(
            ActiveMQExceptionType.IO_ERROR.getCode(), "Kafka Journal is in a failed state");
      }
      throw new ActiveMQException("Kafka Journal is in a failed state");
    }
  }

  private void appendRecord(JournalRecord record) throws Exception {
    KafkaJournalRecord kjr = new KafkaJournalRecord(record);
    appendRecord(kjr);
  }

  private void appendRecord(JournalRecord record, boolean sync) throws Exception {
    KafkaJournalRecord kjr = new KafkaJournalRecord(record).setSync(sync);
    appendRecord(kjr);
  }

  private void appendRecord(JournalRecord record, boolean sync, IOCompletion ioCompletion)
      throws Exception {

    appendRecord(record, sync, ioCompletion, true);
  }

  private void appendRecord(
      JournalRecord record, boolean sync, IOCompletion ioCompletion, boolean lineUpContext)
      throws Exception {

    KafkaJournalRecord kjr =
        new KafkaJournalRecord(record)
            .setIoCompletion(ioCompletion)
            .setStoreLineUp(lineUpContext)
            .setSync(sync);

    appendRecord(kjr);
  }

  private void appendRecord(KafkaJournalRecord kjr) throws Exception {
    SLOG.trace(b -> b.event("AppendRecord").name(journalName).addJournalRecord(kjr.getRecord()));

    if (kjr.isStoreLineUp() && kjr.getIoCompletion() != null) {
      kjr.getIoCompletion().storeLineUp();
    }
    checkStatus(kjr.getIoCompletion());

    SimpleWaitIOCallback callback = null;
    if (kjr.isSync() && kjr.getIoCompletion() == null) {
      callback = new SimpleWaitIOCallback();
      kjr.setIoCompletion(callback);
    }

    publishJournalRecord(kjr);

    if (callback != null) {
      callback.waitCompletion();
    }
  }

  /**
   * Publishes a journal record to kafka by putting it into the journal cache. This put is a
   * blocking operation as it flushing to kafka on each put.
   *
   * <p>Errors while publishing to kafka gets propagated to criticalIOErrorListener
   *
   * @param kjr the KafkaJournalRecord to publish
   */
  protected void publishJournalRecord(KafkaJournalRecord kjr) {
    JournalEntry journalEntry =
        JournalEntry.newBuilder().setAppendedRecord(kjr.getRecord()).build();
    try {
      KafkaCache.Metadata metadata =
          journalCache.put(null, kjr.getKafkaMessageKey(), journalEntry, false);
      SLOG.trace(
          b ->
              b.event("PublishJournalRecord")
                  .markSuccess()
                  .addRecordMetadata(metadata.getRecordMetadata())
                  .addJournalEntryKey(kjr.getKafkaMessageKey())
                  .addJournalEntry(journalEntry));
      if (kjr.getIoCompletion() != null) kjr.getIoCompletion().done();
    } catch (CacheException e) {
      SLOG.error(
          b ->
              b.event("PublishJournalRecord")
                  .markFailure()
                  .message("Failed to publish journal record")
                  .addJournalEntryKey(kjr.getKafkaMessageKey())
                  .addJournalEntry(journalEntry));
      if (kjr.getIoCompletion() != null) {
        handleException(e);
        kjr.getIoCompletion().onError(ActiveMQExceptionType.IO_ERROR.getCode(), e.getMessage());
      }
    }
  }

  @Override
  public void appendAddRecord(long id, byte protocolRecordType, byte[] record, boolean sync)
      throws Exception {
    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(record))
            .build();

    appendRecord(r, sync);
  }

  @Override
  public void appendAddRecord(
      long id, byte protocolRecordType, Persister persister, Object record, boolean sync)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(decode(persister, record)))
            .build();

    appendRecord(r, sync);
  }

  @Override
  public void appendAddRecord(
      long id,
      byte protocolRecordType,
      Persister persister,
      Object record,
      boolean sync,
      IOCompletion completionCallback)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(decode(persister, record)))
            .build();

    appendRecord(r, sync, completionCallback);
  }

  @Override
  public void appendUpdateRecord(long id, byte protocolRecordType, byte[] record, boolean sync)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(record))
            .build();

    appendRecord(r, sync);
  }

  @Override
  public void appendUpdateRecord(
      long id, byte protocolRecordType, Persister persister, Object record, boolean sync)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(decode(persister, record)))
            .build();

    appendRecord(r, sync);
  }

  @Override
  public void appendUpdateRecord(
      long id,
      byte protocolRecordType,
      Persister persister,
      Object record,
      boolean sync,
      IOCompletion callback)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(decode(persister, record)))
            .build();

    appendRecord(r, sync, callback);
  }

  @Override
  public void tryAppendUpdateRecord(
      long id,
      byte recordType,
      byte[] record,
      JournalUpdateCallback updateCallback,
      boolean sync,
      boolean replaceableRecord)
      throws Exception {

    appendUpdateRecord(id, recordType, record, sync);
  }

  @Override
  public void tryAppendUpdateRecord(
      long id,
      byte recordType,
      Persister persister,
      Object record,
      boolean sync,
      boolean replaceableUpdate,
      JournalUpdateCallback updateCallback,
      IOCompletion completionCallback)
      throws Exception {

    appendUpdateRecord(id, recordType, persister, record, sync, completionCallback);
  }

  @Override
  public void tryAppendUpdateRecord(
      long id,
      byte recordType,
      Persister persister,
      Object record,
      JournalUpdateCallback updateCallback,
      boolean sync,
      boolean replaceableUpdate)
      throws Exception {
    appendUpdateRecord(id, recordType, persister, record, sync);
  }

  @Override
  public void appendDeleteRecord(long id, boolean sync) throws Exception {

    // delete record marks the record as eligible for compaction
    appendDeleteRecord(id, sync, null);
  }

  @Override
  public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback)
      throws Exception {

    if (SLOG.logger().isTraceEnabled()) {
      SLOG.logger().trace("scheduling appendDeleteRecord::id={}", id);
    }

    // delete record marks the record as eligible for compaction
    JournalRecord r =
        JournalRecord.newBuilder()
            .setMessageId(id)
            .setRecordType(JournalRecordType.DELETE_RECORD)
            .build();

    appendRecord(r, sync, completionCallback);
  }

  @Override
  public void tryAppendDeleteRecord(
      long id, boolean sync, JournalUpdateCallback updateCallback, IOCompletion completionCallback)
      throws Exception {

    appendDeleteRecord(id, sync);
  }

  @Override
  public void tryAppendDeleteRecord(
      long id, JournalUpdateCallback journalUpdateCallback, boolean sync) throws Exception {

    appendDeleteRecord(id, sync);
  }

  @Override
  public void appendAddRecordTransactional(
      long txID, long id, byte protocolRecordType, Persister persister, Object record)
      throws Exception {

    appendAddRecordTransactional(txID, id, protocolRecordType, decode(persister, record));
  }

  @Override
  public void appendAddRecordTransactional(
      long txID, long id, byte protocolRecordType, byte[] record) throws Exception {
    // adds a new record to the journal

    JournalRecord r =
        JournalRecord.newBuilder()
            .setTxId(txID)
            .setMessageId(id)
            .setRecordType(JournalRecordType.ADD_RECORD_TX)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(record))
            .build();

    appendRecord(r);
  }

  @Override
  public void appendUpdateRecordTransactional(
      long txID, long id, byte protocolRecordType, Persister persister, Object record)
      throws Exception {

    byte[] recordBytes = decode(persister, record);
    appendUpdateRecordTransactional(txID, id, protocolRecordType, recordBytes);
  }

  @Override
  public void appendUpdateRecordTransactional(
      long txID, long id, byte protocolRecordType, byte[] record) throws Exception {

    // add a record indicating an existing record has been updated.  Not sure if this should replace
    // the previous record or be aggregated with it.

    JournalRecord r =
        JournalRecord.newBuilder()
            .setTxId(txID)
            .setMessageId(id)
            .setRecordType(JournalRecordType.ANNOTATE_RECORD_TX)
            .setProtocolRecordType(protocolRecordType)
            .setData(ByteString.copyFrom(record))
            .build();

    appendRecord(r);
  }

  @Override
  public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record)
      throws Exception {

    appendDeleteRecordTransactional(txID, id, decode(record));
  }

  @Override
  public void appendDeleteRecordTransactional(long txID, long id) throws Exception {

    JournalRecord r = JournalRecord.newBuilder().setTxId(txID).setMessageId(id).build();

    appendRecord(r);
  }

  @Override
  public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setTxId(txID)
            .setMessageId(id)
            .setData(ByteString.copyFrom(record))
            .build();

    appendRecord(r);
  }

  @Override
  public void appendCommitRecord(long txID, boolean sync) throws Exception {

    appendCommitRecord(txID, sync, null, true);
  }

  @Override
  public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception {

    appendCommitRecord(txID, sync, callback, true);
  }

  @Override
  public void appendCommitRecord(
      long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder().setRecordType(JournalRecordType.COMMIT_TX).setTxId(txID).build();

    appendRecord(r, sync, callback, lineUpContext);
  }

  @Override
  public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync)
      throws Exception {

    appendPrepareRecord(txID, transactionData, sync, null);
  }

  @Override
  public void appendPrepareRecord(
      long txID, EncodingSupport transactionData, boolean sync, IOCompletion callback)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.PREPARE_TX)
            .setTxId(txID)
            .setData(ByteString.copyFrom(decode(transactionData)))
            .build();

    appendRecord(r, sync, callback);
  }

  @Override
  public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.PREPARE_TX)
            .setTxId(txID)
            .setData(ByteString.copyFrom(transactionData))
            .build();

    appendRecord(r, sync);
  }

  @Override
  public void appendRollbackRecord(long txID, boolean sync) throws Exception {

    appendRollbackRecord(txID, sync, null);
  }

  @Override
  public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback)
      throws Exception {

    JournalRecord r =
        JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ROLLBACK_TX)
            .setTxId(txID)
            .build();

    appendRecord(r, sync, callback);
  }

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
  public void lineUpContext(IOCompletion ioCompletion) {
    ioCompletion.storeLineUp();
  }

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
  public void start() throws Exception {
    state = JournalState.STARTED;
    SLOG.debug(b -> b.name(journalName).event("Start").markSuccess());
  }

  @Override
  public void stop() throws Exception {
    state = JournalState.STOPPED;
    SLOG.debug(b -> b.name(journalName).event("Stop").markSuccess());
  }

  @Override
  public boolean isStarted() {
    return state == JournalState.STARTED || state == JournalState.LOADED;
  }

  /**
   * Indicates that the journal is ready to accept writes, meaning it has been started and loaded.
   */
  public boolean isReady() {
    // kcache does not have an indication of whether it's ready.
    // We can hack it by performing a simple get operation which would throw an CacheException
    // indicate that the store is not ready yet (not initialized).
    try {
      journalCache.get("dummy");
      return true;
    } catch (CacheException e) {
      return false;
    }
  }
}
