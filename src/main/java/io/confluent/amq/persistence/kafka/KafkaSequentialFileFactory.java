package io.confluent.amq.persistence.kafka;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

public class KafkaSequentialFileFactory implements SequentialFileFactory {
    @Override
    public SequentialFile createSequentialFile(String fileName) {
        return null;
    }

    @Override
    public int getMaxIO() {
        return 0;
    }

    @Override
    public List<String> listFiles(String extension) throws Exception {
        return null;
    }

    @Override
    public boolean isSupportsCallbacks() {
        return false;
    }

    @Override
    public void onIOError(Exception exception, String message, SequentialFile file) {

    }

    @Override
    public ByteBuffer allocateDirectBuffer(int size) {
        return null;
    }

    @Override
    public void releaseDirectBuffer(ByteBuffer buffer) {

    }

    @Override
    public ByteBuffer newBuffer(int size) {
        return null;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {

    }

    @Override
    public void activateBuffer(SequentialFile file) {

    }

    @Override
    public void deactivateBuffer() {

    }

    @Override
    public ByteBuffer wrapBuffer(byte[] bytes) {
        return null;
    }

    @Override
    public int getAlignment() {
        return 0;
    }

    @Override
    public SequentialFileFactory setAlignment(int alignment) {
        return null;
    }

    @Override
    public int calculateBlockSize(int bytes) {
        return 0;
    }

    @Override
    public File getDirectory() {
        return null;
    }

    @Override
    public void clearBuffer(ByteBuffer buffer) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void createDirs() throws Exception {

    }

    @Override
    public void flush() {

    }

    @Override
    public SequentialFileFactory setDatasync(boolean enabled) {
        return null;
    }

    @Override
    public boolean isDatasync() {
        return false;
    }

    @Override
    public long getBufferSize() {
        return 0;
    }
}
