/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecord.JournalRecordType;
import io.confluent.amq.persistence.kafka.JournalRecord.UserRecordType;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.KafkaJournalRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  public static String journalTopic(String bridgeId, String journalName) {
    return "_jms.bridge_" + bridgeId.toLowerCase() + "_" + journalName.toLowerCase();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJournal.class);
  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaJournal.class));

  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;
  public static final int SEGMENT_MAX_BYTES = 100 * 1024 * 1024;

  private final AtomicBoolean failed = new AtomicBoolean(false);

  private final KafkaIO kafkaIO;
  private final String bridgeId;
  private final String journalName;
  private final ExecutorFactory executor;
  private final IOCriticalErrorListener criticalIOErrorListener;

  private final String destTopic;

  private volatile JournalState state;
  private KafkaJournalProcessor processor;

  public KafkaJournal(KafkaIO kafkaIO,
      String bridgeId,
      String journalName,
      ExecutorFactory executor,
      IOCriticalErrorListener criticalIOErrorListener) {

    this.kafkaIO = kafkaIO;
    this.bridgeId = bridgeId;
    this.journalName = journalName;
    this.executor = executor;
    this.criticalIOErrorListener = criticalIOErrorListener;

    this.destTopic = journalTopic(this.bridgeId, this.journalName);
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
      throw new ActiveMQShutdownException("Kafka Journal is not loaded");
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
    checkStatus(kjr.getIoCompletion());

    SLOG.trace(b -> b.event("AppendRecord").name(journalName).addJournalRecord(kjr.getRecord()));

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
    kafkaIO.withProducer((kafkaProducer) -> {

      ProducerRecord<Message, Message> producerRecord = new ProducerRecord<>(
          record.getDestTopic(),
          record.getKafkaMessageKey(),
          record.getRecord());

      kafkaProducer.send(producerRecord, (meta, err) -> {
        if (err != null) {
          if (record.getIoCompletion() != null) {
            handleException(err);
            record.getIoCompletion()
                .onError(ActiveMQExceptionType.IO_ERROR.getCode(), err.getMessage());
          }

          SLOG.error(b -> b
              .event("PublishJournalRecord")
              .markFailure()
              .addProducerRecord(producerRecord)
              .addJournalRecordKey(record.getKafkaMessageKey())
              .addJournalRecord(record.getRecord()), err);

        } else {

          SLOG.trace(b -> b
              .event("PublishJournalRecord")
              .markSuccess()
              .addRecordMetadata(meta)
              .addJournalRecordKey(record.getKafkaMessageKey())
              .addJournalRecord(record.getRecord()));
        }

        if (record.getIoCompletion() != null) {
          record.getIoCompletion().done();
        }
      });
      return null;
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

  private void appendRecord(JournalRecord record, boolean sync, IOCompletion ioCompletion)
      throws Exception {

    KafkaJournalRecord kjr = new KafkaJournalRecord(record)
        .setDestTopic(this.destTopic)
        .setIoCompletion(ioCompletion)
        .setSync(sync);

    appendRecord(kjr);
  }


  @Override
  public void start() throws Exception {
    SLOG.debug(b -> b.name(journalName).event("Start"));

    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    topicProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(SEGMENT_MAX_BYTES));

    // default to 1MB plus extra space for extra byte flags
    //todo: fix these things so they are configurable
    topicProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(MAX_RECORD_SIZE + 1024));
    kafkaIO.createTopicIfNotExists(this.destTopic, 1, 1, topicProps);

    Properties streamProps = this.kafkaIO.getKafkaProps();
    streamProps.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        String.format("jms.bridge.%s.%s", this.bridgeId, this.journalName));

    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
    streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    //requires 3 brokers at minimum
    //streamProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    this.processor = new KafkaJournalProcessor(this.destTopic, streamProps);
    this.processor.init();

    state = JournalState.STARTED;
    SLOG.debug(b -> b.name(journalName).event("Start").markSuccess());
  }

  @Override
  public void stop() throws Exception {
    SLOG.debug(b -> b.name(journalName).event("Stop"));
    if (this.processor != null) {
      this.processor.stop();
    }
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
    return state == JournalState.LOADED;
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
  public synchronized JournalLoadInformation load(final List<RecordInfo> committedRecords,
      final List<PreparedTransactionInfo> preparedTransactions,
      final TransactionFailureCallback failureCallback,
      final boolean fixBadTX) throws Exception {

    KafkaJournalLoaderCallback lc = KafkaJournalLoaderCallback.from(committedRecords,
        preparedTransactions, failureCallback, fixBadTX);

    return load(lc);
  }

  @Override
  public synchronized JournalLoadInformation load(LoaderCallback reloadManager) {

    SLOG.debug(b -> b.name(journalName).event("Load"));

    KafkaJournalLoaderCallback klc = KafkaJournalLoaderCallback.wrap(reloadManager);
    processor.startAndLoad(klc);

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
  public boolean tryAppendDeleteRecord(long id, boolean sync, IOCompletion completionCallback)
      throws Exception {

    appendDeleteRecord(id, sync, completionCallback);
    return true;
  }

  @Override
  public void appendAddRecord(long id, byte userRecordType, byte[] record, boolean sync)
      throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.ADD_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r, sync);

  }


  @Override
  public void appendAddRecord(long id, byte userRecordType, Persister persister, Object record,
      boolean sync) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.ADD_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync);

  }


  @Override
  public void appendAddRecord(long id, byte userRecordType, Persister persister, Object record,
      boolean sync, IOCompletion completionCallback) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.ADD_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync, completionCallback);

  }


  @Override
  public void appendUpdateRecord(long id, byte userRecordType, byte[] record, boolean sync)
      throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.UPDATE_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r, sync);

  }


  @Override
  public void appendUpdateRecord(long id, byte userRecordType, Persister persister, Object record,
      boolean sync) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.UPDATE_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(decode(persister, record)))
        .build();

    appendRecord(r, sync);

  }


  @Override
  public void appendUpdateRecord(long id, byte userRecordType, Persister persister, Object record,
      boolean sync, IOCompletion callback) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.UPDATE_RECORD)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
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
    //delete record marks the record as eligible for compaction
    JournalRecord r = JournalRecord.newBuilder()
        .setId(id)
        .setRecordType(JournalRecordType.DELETE_RECORD)
        .build();

    appendRecord(r, sync, completionCallback);
  }


  @Override
  public void appendAddRecordTransactional(long txID, long id, byte userRecordType,
      Persister persister,
      Object record) throws Exception {

    appendAddRecordTransactional(txID, id, userRecordType, decode(persister, record));
  }


  @Override
  public void appendAddRecordTransactional(long txID, long id, byte userRecordType, byte[] record)
      throws Exception {
    //adds a new record to the journal

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setId(id)
        .setRecordType(JournalRecordType.ADD_RECORD_TX)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r);
  }


  @Override
  public void appendUpdateRecordTransactional(long txID, long id, byte userRecordType,
      Persister persister, Object record) throws Exception {

    byte[] recordBytes = decode(persister, record);
    appendUpdateRecordTransactional(txID, id, userRecordType, recordBytes);
  }


  @Override
  public void appendUpdateRecordTransactional(long txID, long id, byte userRecordType,
      byte[] record)
      throws Exception {

    //add a record indicating an existing record has been updated.  Not sure if this should replace
    //the previous record or be aggregated with it.

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setId(id)
        .setRecordType(JournalRecordType.UPDATE_RECORD_TX)
        .setUserRecordType(UserRecordType.forNumber(userRecordType))
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
        .setId(id)
        .build();

    appendRecord(r);
  }


  @Override
  public void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setTxId(txID)
        .setId(id)
        .setData(ByteString.copyFrom(record))
        .build();

    appendRecord(r);
  }


  @Override
  public void appendCommitRecord(long txID, boolean sync) throws Exception {

    appendCommitRecord(txID, sync, null, false);
  }


  @Override
  public void appendCommitRecord(long txID, boolean sync, IOCompletion callback) throws Exception {

    appendCommitRecord(txID, sync, callback, false);
  }


  @Override
  public void appendCommitRecord(long txID, boolean sync, IOCompletion callback,
      boolean lineUpContext) throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.COMMIT_RECORD)
        .setTxId(txID)
        .build();

    appendRecord(r, sync, callback);
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
        .setRecordType(JournalRecordType.PREPARE_RECORD)
        .setTxId(txID)
        .setData(ByteString.copyFrom(decode(transactionData)))
        .build();

    appendRecord(r, sync, callback);
  }


  @Override
  public void appendPrepareRecord(long txID, byte[] transactionData, boolean sync)
      throws Exception {

    JournalRecord r = JournalRecord.newBuilder()
        .setRecordType(JournalRecordType.PREPARE_RECORD)
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
        .setRecordType(JournalRecordType.ROLLBACK_RECORD)
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
  public boolean tryAppendDeleteRecord(long id, boolean sync) throws Exception {

    appendDeleteRecord(id, sync);
    return true;
  }


  @Override
  public boolean tryAppendUpdateRecord(long id, byte recordType, Persister persister, Object record,
      boolean sync) throws Exception {

    appendUpdateRecord(id, recordType, persister, record, sync);
    return true;
  }


  @Override
  public boolean tryAppendUpdateRecord(long id, byte recordType, byte[] record, boolean sync)
      throws Exception {

    appendUpdateRecord(id, recordType, record, sync);
    return true;
  }


  @Override
  public boolean tryAppendUpdateRecord(long id, byte recordType, Persister persister, Object record,
      boolean sync, IOCompletion callback) throws Exception {

    appendUpdateRecord(id, recordType, persister, record, sync, callback);
    return true;
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
  public void runDirectJournalBlast() throws Exception {
    SLOG.debug(b -> b
        .name(journalName)
        .event("UnimplementedMethodCall")
        .message("runDirectJournalBlast"));
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


}
