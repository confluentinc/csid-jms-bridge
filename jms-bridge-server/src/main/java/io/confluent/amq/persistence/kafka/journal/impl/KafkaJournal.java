/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.protobuf.ByteString;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KafkaJournalRecord;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQShutdownException;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.JournalUpdateCallback;
import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.SimpleWaitIOCallback;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * <p>
 * A journal that is implemented on top of Apache Kafka.
 * </p>
 * <p>
 * Journal state:
 * </p>
 * <pre>
 *  null -> started -> loaded -> stopped
 * </pre>
 */
//note, exceptions should not really be part of the data coupling check
//note, I cannot satisfy checkstyle's overrides ordering requirements
@SuppressWarnings({"unchecked", "rawtypes", "checkstyle:ClassDataAbstractionCoupling",
    "checkstyle:OverloadMethodsDeclarationOrder"})
public class KafkaJournal implements Journal {

  private static final String TOPIC_FORMAT = "_jms.bridge_%s_%s_%s";

  public static String journalWalTopic(String bridgeId, String journalName) {
    return String.format(
        TOPIC_FORMAT,
        bridgeId.toLowerCase(),
        journalName.toLowerCase(),
        "wal");
  }

  public static String journalTableTopic(String bridgeId, String journalName) {
    return String.format(
        TOPIC_FORMAT,
        bridgeId.toLowerCase(),
        journalName.toLowerCase(),
        "tbl");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJournal.class);
  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaJournal.class));

  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;

  private final AtomicBoolean failed = new AtomicBoolean(false);

  private final KafkaIO kafkaIO;
  private final String journalName;
  private final ExecutorFactory executor;
  private final IOCriticalErrorListener criticalIOErrorListener;

  private final String destTopic;

  private volatile JournalState state;
  private final KJournal processor;

  public KafkaJournal(
      KJournal processor,
      KafkaIO kafkaIO,
      ExecutorFactory executor,
      IOCriticalErrorListener criticalIOErrorListener) {

    this.processor = processor;
    this.kafkaIO = kafkaIO;
    this.journalName = processor.name();
    this.executor = executor;
    this.criticalIOErrorListener = criticalIOErrorListener;
    this.destTopic = processor.walTopic();
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
    criticalIOErrorListener
        .onIOException(e, "Critical IO Error.  Failed to process Kafka Records", null);
  }

  private void checkStatus(IOCompletion callback) throws Exception {
    if (!isReady()) {
      if (callback != null) {
        callback
            .onError(ActiveMQExceptionType.ILLEGAL_STATE.getCode(), "Kafka Journal is not loaded");
      }
      throw new ActiveMQShutdownException("Kafka Journal is not ready");
    }

    if (failed.get()) {
      if (callback != null) {
        callback.onError(ActiveMQExceptionType.IO_ERROR.getCode(),
            "Kafka Journal is in a failed state");
      }
      throw new ActiveMQException("Kafka Journal is in a failed state");
    }
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

  protected void publishJournalRecord(KafkaJournalRecord record) {
    ProducerRecord<JournalEntryKey, JournalEntry> producerRecord = new ProducerRecord<>(
        record.getDestTopic(),
        record.getKafkaMessageKey(),
        JournalEntry.newBuilder().setAppendedRecord(record.getRecord()).build());

    kafkaIO.getInternalProducer()
        .send(producerRecord, (meta, err) -> {

          SLOG.trace(b -> b
              .event("PublishJournalRecord")
              .markSuccess()
              .addRecordMetadata(meta)
              .addJournalEntryKey(producerRecord.key())
              .addJournalEntry(producerRecord.value()));

          if (err == null) {
            if (record.getIoCompletion() != null) {
              record.getIoCompletion().done();
            }
          } else {

            if (record.getIoCompletion() != null) {
              handleException(err);
              record.getIoCompletion()
                  .onError(ActiveMQExceptionType.IO_ERROR.getCode(), err.getMessage());
            }

            SLOG.error(b -> b
                .event("PublishJournalRecord")
                .markFailure()
                .addProducerRecord(producerRecord)
                .addJournalEntryKey(producerRecord.key())
                .addJournalEntry(producerRecord.value()), err);

            criticalIOErrorListener.onIOException(err, "Failed to write to Kafka", null);
          }
        });
  }

  private void appendRecord(JournalRecord record) throws Exception {
    KafkaJournalRecord kjr = new KafkaJournalRecord(record)
        .setDestTopic(this.destTopic);

    appendRecord(kjr);
  }

  private void appendRecord(JournalRecord record, boolean sync) throws Exception {
    KafkaJournalRecord kjr = new KafkaJournalRecord(record)
        .setDestTopic(this.destTopic)
        .setSync(sync);

    appendRecord(kjr);
  }

  private void appendRecord(
      JournalRecord record, boolean sync, IOCompletion ioCompletion)
      throws Exception {

    appendRecord(record, sync, ioCompletion, true);
  }

  private void appendRecord(
      JournalRecord record, boolean sync, IOCompletion ioCompletion, boolean lineUpContext)
      throws Exception {

    KafkaJournalRecord kjr = new KafkaJournalRecord(record)
        .setDestTopic(this.destTopic)
        .setIoCompletion(ioCompletion)
        .setStoreLineUp(lineUpContext)
        .setSync(sync);

    appendRecord(kjr);
  }

  @Override
  public void start() throws Exception {
    state = JournalState.STARTED;
    SLOG.debug(b -> b.name(journalName).event("Start").markSuccess());
  }

  @Override
  public void stop() throws Exception {
    this.state = JournalState.STOPPED;
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
    return processor.isRunning();
  }

  @Override
  public long getMaxRecordSize() {
    return MAX_RECORD_SIZE;
  }

  @Override
  public synchronized JournalLoadInformation load(
      final SparseArrayLinkedList<RecordInfo> committedRecords,
      final List<PreparedTransactionInfo> preparedTransactions,
      final TransactionFailureCallback failureCallback,
      final boolean fixBadTX) throws Exception {

    final List<RecordInfo> records = new ArrayList<>();
    final JournalLoadInformation journalLoadInformation = load(records, preparedTransactions,
        failureCallback, fixBadTX);
    records.forEach(committedRecords::add);
    return journalLoadInformation;
  }

  @Override
  public synchronized JournalLoadInformation load(
      final List<RecordInfo> committedRecords,
      final List<PreparedTransactionInfo> preparedTransactions,
      final TransactionFailureCallback failureCallback,
      final boolean fixBadTX) {

    KafkaJournalLoaderCallback lc = KafkaJournalLoaderCallback.from(committedRecords,
        preparedTransactions, failureCallback, fixBadTX);

    return load(lc);
  }

  @Override
  public synchronized JournalLoadInformation load(LoaderCallback reloadManager) {

    SLOG.debug(b -> b.name(journalName).event("Load"));

    KafkaJournalLoaderCallback klc = KafkaJournalLoaderCallback.wrap(reloadManager);
    processor.load(klc);

    JournalLoadInformation jli = klc.getLoadInfo();
    this.state = JournalState.LOADED;

    SLOG.debug(b -> b
        .name(journalName)
        .event("Load")
        .markSuccess()
        .putTokens("NumberOfRecords", jli.getNumberOfRecords())
        .putTokens("MaxID", jli.getMaxID()));

    return jli;
  }

  @Override
  public void appendAddRecord(long id, byte protocolRecordType, byte[] record, boolean sync)
      throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.ADD_RECORD)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r, sync);

  }

  @Override
  public void appendAddRecord(long id, byte protocolRecordType, Persister persister, Object record,
                              boolean sync) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.ADD_RECORD)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync);

  }

  @Override
  public void appendAddRecord(long id, byte protocolRecordType, Persister persister, Object record,
                              boolean sync, IOCompletion completionCallback) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
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

    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.ANNOTATE_RECORD)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r, sync);

  }

  @Override
  public void appendUpdateRecord(long id, byte protocolRecordType, Persister persister,
                                 Object record,
                                 boolean sync) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.ANNOTATE_RECORD)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync);

  }

  @Override
  public void appendUpdateRecord(long id, byte protocolRecordType, Persister persister,
                                 Object record,
                                 boolean sync, IOCompletion callback) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.ANNOTATE_RECORD)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync, callback);

  }

  @Override
  public void appendDeleteRecord(long id, boolean sync) throws Exception {

    //delete record marks the record as eligible for compaction
    appendDeleteRecord(id, sync, null);

  }

  @Override
  public void appendDeleteRecord(long id, boolean sync, IOCompletion completionCallback)
      throws Exception {

    if (SLOG.logger().isTraceEnabled()) {
      SLOG.logger().trace("scheduling appendDeleteRecord::id={}", id);
    }

    //delete record marks the record as eligible for compaction
    JournalRecord r = JournalRecord.newBuilder()
        .setMessageId(id)
        .setRecordType(JournalRecordType.DELETE_RECORD)
        .build();

    appendRecord(r, sync, completionCallback);
  }

  @Override
  public void appendAddRecordTransactional(long txID, long id, byte protocolRecordType,
                                           Persister persister,
                                           Object record) throws Exception {

    appendAddRecordTransactional(txID, id, protocolRecordType, decode(persister, record));
  }

  @Override
  public void appendAddRecordTransactional(long txID, long id, byte protocolRecordType,
                                           byte[] record)
      throws Exception {
    //adds a new record to the journal

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setMessageId(id)
        .setRecordType(JournalRecordType.ADD_RECORD_TX)
        .setProtocolRecordType(protocolRecordType)
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r);
  }

  @Override
  public void appendUpdateRecordTransactional(long txID, long id, byte protocolRecordType,
                                              Persister persister, Object record) throws Exception {

    byte[] recordBytes = decode(persister, record);
    appendUpdateRecordTransactional(txID, id, protocolRecordType, recordBytes);
  }

  @Override
  public void appendUpdateRecordTransactional(long txID, long id, byte protocolRecordType,
                                              byte[] record)
      throws Exception {

    //add a record indicating an existing record has been updated.  Not sure if this should replace
    //the previous record or be aggregated with it.

    JournalRecord r = JournalRecord.newBuilder()
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

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setMessageId(id)
        .setRecordType(JournalRecordType.DELETE_RECORD_TX)
        .build();

    appendRecord(r);
  }

  @Override
  public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setMessageId(id)
        .setData(ByteString.copyFrom(record))
        .setRecordType(JournalRecordType.DELETE_RECORD_TX)
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
  public void appendCommitRecord(long txID, boolean sync, IOCompletion callback,
                                 boolean lineUpContext) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.COMMIT_TX)
        .setTxId(txID)
        .build();

    appendRecord(r, sync, callback, lineUpContext);
  }

  @Override
  public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync)
      throws Exception {

    appendPrepareRecord(txID, transactionData, sync, null);
  }

  @Override
  public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync,
                                  IOCompletion callback) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.PREPARE_TX)
        .setTxId(txID)
        .setData(ByteString.copyFrom(decode(transactionData)))
        .build();

    appendRecord(r, sync, callback);
  }

  @Override
  public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync)
      throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
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

    JournalRecord r = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.ROLLBACK_TX)
        .setTxId(txID)
        .build();

    appendRecord(r, sync, callback);
  }

  @Override
  public JournalLoadInformation loadInternalOnly() throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("loadInternalOnly"));

    return null;
  }

  @Override
  public JournalLoadInformation loadSyncOnly(JournalState state) throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("loadSyncOnly"));
    return null;
  }

  @Override
  public void lineUpContext(IOCompletion callback) {
    callback.storeLineUp();
  }

  @Override
  public void tryAppendDeleteRecord(long id, boolean sync, JournalUpdateCallback updateCallback,
                                    IOCompletion completionCallback) throws Exception {

    appendDeleteRecord(id, sync);
  }


  @Override
  public void tryAppendDeleteRecord(long id, JournalUpdateCallback journalUpdateCallback,
                                    boolean sync) throws Exception {

    appendDeleteRecord(id, sync);
  }

  @Override
  public void tryAppendUpdateRecord(long id, byte recordType, byte[] record,
                                    JournalUpdateCallback updateCallback, boolean sync,
                                    boolean replaceableRecord) throws Exception {

    appendUpdateRecord(id, recordType, record, sync);
  }

  @Override
  public void tryAppendUpdateRecord(long id,
                                    byte recordType,
                                    Persister persister,
                                    Object record,
                                    boolean sync,
                                    boolean replaceableUpdate,
                                    JournalUpdateCallback updateCallback,
                                    IOCompletion completionCallback) throws Exception {


    appendUpdateRecord(id, recordType, persister, record, sync, completionCallback);
  }

  @Override
  public void tryAppendUpdateRecord(long id, byte recordType, Persister persister, Object record,
                                    JournalUpdateCallback updateCallback, boolean sync,
                                    boolean replaceableUpdate) throws Exception {

    appendUpdateRecord(id, recordType, persister, record, sync);
  }

  @Override
  public int getAlignment() throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("getAlignment"));

    // required for replication, not needed so return 0
    return 0;
  }

  @Override
  public int getNumberOfRecords() {
    return 0;
  }

  @Override
  public int getUserVersion() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("getUserVersion"));
    return 0;
  }

  @Override
  public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("createFilesForBackupSync"));

    return Collections.emptyMap();
  }

  @Override
  public void synchronizationLock() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("synchronizationLock"));
    //do nothing
  }

  @Override
  public void synchronizationUnlock() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("synchronizationUnlock"));
    //do nothing
  }

  @Override
  public void forceMoveNextFile() throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("forceMoveNextFile"));
    //do nothing
  }

  @Override
  public JournalFile[] getDataFiles() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("getDataFiles"));
    return new JournalFile[0];
  }

  @Override
  public SequentialFileFactory getFileFactory() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("getFileFactory"));
    return null;
  }

  @Override
  public int getFileSize() {
    return 0;
  }

  @Override
  public void scheduleCompactAndBlock(int timeout) throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("scheduleCompactAndBlock"));
    //do nothing
  }

  @Override
  public void replicationSyncPreserveOldFiles() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("scheduleCompactAndBlock"));
    //do nothing
  }

  @Override
  public void replicationSyncFinished() {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("replicationSyncFinished"));
    //do nothing
  }

  @Override
  public void flush() throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("flush"));
    //do nothing
  }

  @Override
  public void setRemoveExtraFilesOnLoad(boolean removeExtraFilesOnLoad) {
    //no files
  }

  @Override
  public boolean isRemoveExtraFilesOnLoad() {
    return false;
  }

  /**
   * An event is data recorded on the journal, but it won't have any weight or deletes. It's always
   * ready to be removed. It is useful on recovery data while in use with backup history journal.
   */
  @Override
  public void appendAddEvent(
      long id, byte recordType, Persister persister, Object record, boolean sync,
      IOCompletion completionCallback) throws Exception {

    //nothing to be done
  }
}
