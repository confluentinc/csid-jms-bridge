package io.confluent.amq.persistence.kafka;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.buffer.TimedBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFile;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KafkaSequentialFile implements SequentialFile {
    private static final Logger logger = Logger.getLogger(KafkaSequentialFile.class);

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void open(int maxIO, boolean useExecutor) throws Exception {

    }

    @Override
    public ByteBuffer map(int position, long size) throws IOException {
        return null;
    }

    @Override
    public boolean fits(int size) {
        return false;
    }

    @Override
    public int calculateBlockStart(int position) throws Exception {
        return 0;
    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public void fill(int size) throws Exception {

    }

    @Override
    public void delete() throws IOException, InterruptedException, ActiveMQException {

    }

    @Override
    public void write(ActiveMQBuffer bytes, boolean sync, IOCallback callback) throws Exception {

    }

    @Override
    public void write(ActiveMQBuffer bytes, boolean sync) throws Exception {

    }

    @Override
    public void write(EncodingSupport bytes, boolean sync, IOCallback callback) throws Exception {

    }

    @Override
    public void write(EncodingSupport bytes, boolean sync) throws Exception {

    }

    @Override
    public void writeDirect(ByteBuffer bytes, boolean sync, IOCallback callback) {

    }

    @Override
    public void writeDirect(ByteBuffer bytes, boolean sync) throws Exception {

    }

    @Override
    public void blockingWriteDirect(ByteBuffer bytes, boolean sync, boolean releaseBuffer) throws Exception {

    }

    @Override
    public int read(ByteBuffer bytes, IOCallback callback) throws Exception {
        return 0;
    }

    @Override
    public int read(ByteBuffer bytes) throws Exception {
        return 0;
    }

    @Override
    public void position(long pos) throws IOException {

    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public long size() throws Exception {
        return 0;
    }

    @Override
    public void renameTo(String newFileName) throws Exception {

    }

    @Override
    public SequentialFile cloneFile() {
        return null;
    }

    @Override
    public void copyTo(SequentialFile newFileName) throws Exception {

    }

    @Override
    public void setTimedBuffer(TimedBuffer buffer) {

    }

    @Override
    public File getJavaFile() {
        return null;
    }
}
