package io.confluent.amq.persistence.kafka;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class KafkaJournalRecord {
    // Record types taken from Journal Impl
    static final byte ADD_RECORD = 11;
    static final byte UPDATE_RECORD = 12;
    static final byte ADD_RECORD_TX = 13;
    static final byte UPDATE_RECORD_TX = 14;

    static final byte DELETE_RECORD_TX = 15;
    static final byte DELETE_RECORD = 16;

    static final byte PREPARE_RECORD = 17;
    static final byte COMMIT_RECORD = 18;
    static final byte ROLLBACK_RECORD = 19;

    // Callback and sync operations
    private IOCompletion ioCompletion = null;
    private boolean storeLineUp = true;
    private boolean sync = false;

    // DB Fields for all records
    private Long id;
    private byte recordType;
    private byte compactCount;
    private long txId;

    // DB fields for ADD_RECORD(TX), UPDATE_RECORD(TX),
    private int variableSize;
    protected byte userRecordType;
    private InputStream record;

    // DB Fields for PREPARE_RECORD
    private int txDataSize;
    private InputStream txData;

    // DB Fields for COMMIT_RECORD and PREPARE_RECORD
    private int txCheckNoRecords;

    private boolean isUpdate;

    private boolean isTransactional;

    private long seq;

    public KafkaJournalRecord(Long id, byte recordType, long seq) {
        this.id = id;
        this.recordType = recordType;
        this.seq = seq;
    }

    public IOCompletion getIoCompletion() {
        return ioCompletion;
    }

    public KafkaJournalRecord setIoCompletion(IOCompletion ioCompletion) {
        this.ioCompletion = ioCompletion;
        return this;
    }

    public boolean isStoreLineUp() {
        return storeLineUp;
    }

    public KafkaJournalRecord setStoreLineUp(boolean storeLineUp) {
        this.storeLineUp = storeLineUp;
        return this;
    }

    public boolean isSync() {
        return sync;
    }

    public KafkaJournalRecord setSync(boolean sync) {
        this.sync = sync;
        return this;
    }

    public Long getId() {
        return id;
    }

    public KafkaJournalRecord setId(Long id) {
        this.id = id;
        return this;
    }

    public byte getRecordType() {
        return recordType;
    }

    public KafkaJournalRecord setRecordType(byte recordType) {
        this.recordType = recordType;
        return this;
    }

    public byte getCompactCount() {
        return compactCount;
    }

    public KafkaJournalRecord setCompactCount(byte compactCount) {
        this.compactCount = compactCount;
        return this;
    }

    public long getTxId() {
        return txId;
    }

    public KafkaJournalRecord setTxId(long txId) {
        this.txId = txId;
        return this;
    }

    public int getVariableSize() {
        return variableSize;
    }

    public KafkaJournalRecord setVariableSize(int variableSize) {
        this.variableSize = variableSize;
        return this;
    }

    public byte getUserRecordType() {
        return userRecordType;
    }

    public KafkaJournalRecord setUserRecordType(byte userRecordType) {
        this.userRecordType = userRecordType;
        return this;
    }

    public InputStream getRecord() {
        return record;
    }

    public KafkaJournalRecord setRecord(Persister persister, Object record) {
        this.variableSize = persister.getEncodeSize(record);

        ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(variableSize);
        persister.encode(encodedBuffer, record);
        this.record = new ActiveMQBufferInputStream(encodedBuffer);
        return this;
    }


    public KafkaJournalRecord setRecord(InputStream record) {
        this.record = record;
        return this;
    }

    public KafkaJournalRecord setRecord(byte[] record) {
        if (record != null) {
            this.variableSize = record.length;
            this.record = new ByteArrayInputStream(record);
        }
        return this;
    }

    public int getTxDataSize() {
        return txDataSize;
    }

    public KafkaJournalRecord setTxDataSize(int txDataSize) {
        this.txDataSize = txDataSize;
        return this;
    }

    public InputStream getTxData() {
        return txData;
    }

    public KafkaJournalRecord setTxData(EncodingSupport txData) {
        this.txDataSize = txData.getEncodeSize();

        ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(txDataSize);
        txData.encode(encodedBuffer);
        this.txData = new ActiveMQBufferInputStream(encodedBuffer);

        return this;
    }

    public KafkaJournalRecord setTxData(InputStream txData) {
        this.txData = txData;
        return this;
    }

    public int getTxCheckNoRecords() {
        return txCheckNoRecords;
    }

    public KafkaJournalRecord setTxCheckNoRecords(int txCheckNoRecords) {
        this.txCheckNoRecords = txCheckNoRecords;
        return this;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public KafkaJournalRecord setUpdate(boolean update) {
        isUpdate = update;
        return this;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public KafkaJournalRecord setTransactional(boolean transactional) {
        isTransactional = transactional;
        return this;
    }

    public long getSeq() {
        return seq;
    }

    public KafkaJournalRecord setSeq(long seq) {
        this.seq = seq;
        return this;
    }
}
