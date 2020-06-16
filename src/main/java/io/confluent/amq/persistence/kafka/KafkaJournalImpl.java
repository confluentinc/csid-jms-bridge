package io.confluent.amq.persistence.kafka;

import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.*;
import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.JournalRecord;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.collections.ConcurrentLongHashMap;
import org.apache.activemq.artemis.utils.collections.SparseArrayLinkedList;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaJournalImpl extends JournalImpl implements Journal {
    private final AtomicLong seq = new AtomicLong(0);

    Properties kafkaProps = null;
    KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    KafkaProducer<byte[], byte[]> kafkaProducer = null;
    List<KafkaJournalRecord> records;

    public KafkaJournalImpl(Properties kafkaProps, int fileSize, int minFiles, int poolSize, int compactMinFiles, int compactPercentage, SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO, int userVersion) {
        super(fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, fileFactory, filePrefix, fileExtension, maxAIO, userVersion);
        this.kafkaProps = kafkaProps;
    }

    public KafkaJournalImpl(Properties kafkaProps, int fileSize, int minFiles, int poolSize, int compactMinFiles, int compactPercentage, int journalFileOpenTimeout, SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO, int userVersion) {
        super(fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, journalFileOpenTimeout, fileFactory, filePrefix, fileExtension, maxAIO, userVersion);
        this.kafkaProps = kafkaProps;
    }

    public KafkaJournalImpl(Properties kafkaProps, ExecutorFactory ioExecutors, int fileSize, int minFiles, int poolSize, int compactMinFiles, int compactPercentage, SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO, int userVersion) {
        super(ioExecutors, fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, fileFactory, filePrefix, fileExtension, maxAIO, userVersion);
        this.kafkaProps = kafkaProps;
    }

    public KafkaJournalImpl(Properties kafkaProps, ExecutorFactory ioExecutors, int fileSize, int minFiles, int poolSize, int compactMinFiles, int compactPercentage, int journalFileOpenTimeout, SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO, int userVersion) {
        super(ioExecutors, fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, journalFileOpenTimeout, fileFactory, filePrefix, fileExtension, maxAIO, userVersion);
        this.kafkaProps = kafkaProps;
    }

    public KafkaJournalImpl(Properties kafkaProps, ExecutorFactory ioExecutors, int fileSize, int minFiles, int poolSize, int compactMinFiles, int compactPercentage, int journalFileOpenTimeout, SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO, int userVersion, IOCriticalErrorListener criticalErrorListener) {
        super(ioExecutors, fileSize, minFiles, poolSize, compactMinFiles, compactPercentage, journalFileOpenTimeout, fileFactory, filePrefix, fileExtension, maxAIO, userVersion, criticalErrorListener);
        this.kafkaProps = kafkaProps;
    }

    @Override
    public synchronized void start() {
        super.start();
        kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaProducer = new KafkaProducer<>(kafkaProps);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.close();
            kafkaProducer.close();
        }));
    }

    @Override
    public synchronized void stop() throws Exception {
        super.stop();
        kafkaConsumer.close();
        kafkaProducer.close();
    }

    protected void appendRecord(KafkaJournalRecord r) {
       //TODO: Append the record
    }

    @Override
    public ConcurrentLongHashMap<JournalRecord> getRecords() {
        return super.getRecords();
    }

    @Override
    public void appendAddRecord(long id, byte recordType, Persister persister, Object record, boolean sync, IOCompletion callback) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.ADD_RECORD, seq.incrementAndGet())
                .setUserRecordType(recordType)
                .setRecord(persister, record)
                .setSync(sync)
                .setIoCompletion(callback);
        appendRecord(r);
    }

    @Override
    public void appendUpdateRecord(long id, byte recordType, Persister persister, Object record, boolean sync, IOCompletion callback) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.ADD_RECORD, seq.incrementAndGet())
            .setUserRecordType(recordType)
            .setRecord(persister, record)
            .setSync(sync)
            .setIoCompletion(callback);

        appendRecord(r);
    }

    @Override
    public boolean tryAppendUpdateRecord(long id, byte recordType, Persister persister, Object record, boolean sync, IOCompletion callback) throws Exception {
        appendUpdateRecord(id, recordType, persister, record, sync, callback);
        return true;
    }

    @Override
    public void appendDeleteRecord(long id, boolean sync, IOCompletion callback) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.DELETE_RECORD, seq.incrementAndGet())
            .setSync(sync)
            .setIoCompletion(callback);
        appendRecord(r);
    }

    @Override
    public boolean tryAppendDeleteRecord(long id, boolean sync, IOCompletion callback) throws Exception {
        appendDeleteRecord(id, sync, callback);
        return true;
    }

    @Override
    public void appendAddRecordTransactional(long txID, long id, byte recordType, Persister persister, Object record) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.ADD_RECORD_TX, seq.incrementAndGet())
            .setUserRecordType(recordType)
        	.setRecord(persister, record)
        	.setTxId(txID);
        appendRecord(r);
    }

    @Override
    public void appendUpdateRecordTransactional(long txID, long id, byte recordType, Persister persister, Object record) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.UPDATE_RECORD_TX, seq.incrementAndGet())
                .setUserRecordType(recordType)
                .setRecord(persister, record)
                .setTxId(txID);
        appendRecord(r);
    }

    @Override
    public void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(id, KafkaJournalRecord.DELETE_RECORD_TX, seq.incrementAndGet())
                .setRecord(EncoderPersister.getInstance(), record)
                .setTxId(txID);
        appendRecord(r);
    }

    @Override
    public void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync, IOCompletion callback) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(-1L, KafkaJournalRecord.PREPARE_RECORD, seq.incrementAndGet());
        r.setTxId(txID);
        r.setTxData(transactionData);
        r.setSync(sync);

        appendRecord(r);
    }

    @Override
    public void lineUpContext(IOCompletion callback) {
        callback.storeLineUp();
    }

    @Override
    public void appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(-1L, KafkaJournalRecord.COMMIT_RECORD, seq.incrementAndGet())
            .setTxId(txID)
            .setStoreLineUp(lineUpContext)
            .setIoCompletion(callback)
            .setSync(sync);

        appendRecord(r);
    }

    @Override
    public void appendRollbackRecord(long txID, boolean sync, IOCompletion callback) throws Exception {
        KafkaJournalRecord r = new KafkaJournalRecord(0L, KafkaJournalRecord.ROLLBACK_RECORD, seq.incrementAndGet())
        	.setTxId(txID)
        	.setSync(sync);

        appendRecord(r);
    }
}
