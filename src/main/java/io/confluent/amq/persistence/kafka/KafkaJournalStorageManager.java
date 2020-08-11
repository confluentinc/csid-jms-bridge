/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJournalStorageManager extends JournalStorageManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJournalStorageManager.class);
  private static final String BINDINGS_NAME = "bindings";
  private static final String MESSAGES_NAME = "messages";

  private KafkaIO kafkaIO;

  public KafkaJournalStorageManager(
      JmsBridgeConfiguration config,
      CriticalAnalyzer analyzer, ExecutorFactory executorFactory,
      ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutorFactory) {

    this(config, analyzer, executorFactory, scheduledExecutorService, ioExecutorFactory,
        null);
  }

  public KafkaJournalStorageManager(
      JmsBridgeConfiguration config,
      CriticalAnalyzer analyzer, ExecutorFactory executorFactory,
      ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutorFactory,
      IOCriticalErrorListener criticalErrorListener) {

    super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutorFactory,
        criticalErrorListener);
  }

  @Override
  protected synchronized void init(Configuration config,
      IOCriticalErrorListener criticalErrorListener) {
    //need to create these journals

    JmsBridgeConfiguration jbConfig = (JmsBridgeConfiguration) config;

    if (!jbConfig.getJmsBridgeProperties().containsKey("bridge.id")) {
      throw new IllegalStateException("A bridge id is required for using the Kafka Journal");
    }
    String bridgeId = jbConfig.getJmsBridgeProperties().getProperty("bridge.id");

    this.kafkaIO = new KafkaIO(jbConfig.getJmsBridgeProperties());
    this.kafkaIO.start();

    this.bindingsJournal = new KafkaJournal(kafkaIO, bridgeId, BINDINGS_NAME,
        executorFactory, criticalErrorListener);
    this.messageJournal = new KafkaJournal(kafkaIO, bridgeId, MESSAGES_NAME,
        executorFactory, criticalErrorListener);

  }

  @Override
  public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
    super.stop(ioCriticalError, sendFailover);
    this.kafkaIO.stop();
  }

  @Override
  public ByteBuffer allocateDirectBuffer(int size) {
    return NIOSequentialFileFactory.allocateDirectByteBuffer(size);
  }

  @Override
  public void freeDirectBuffer(ByteBuffer buffer) {
    //nop
  }

  @Override
  public OperationContext getContext() {
    return DummyOperationContext.getInstance();
  }

  private static final class DummyOperationContext implements OperationContext {

    private static DummyOperationContext instance = new DummyOperationContext();

    public static OperationContext getInstance() {
      return DummyOperationContext.instance;
    }

    @Override
    public void executeOnCompletion(final IOCallback runnable) {
      // There are no executeOnCompletion calls while using the DummyOperationContext
      // However we keep the code here for correctness
      runnable.done();
    }

    @Override
    public void executeOnCompletion(IOCallback runnable, boolean storeOnly) {
      executeOnCompletion(runnable);
    }

    @Override
    public void replicationDone() {
    }

    @Override
    public void replicationLineUp() {
    }

    @Override
    public void storeLineUp() {
    }

    @Override
    public void done() {
    }

    @Override
    public void onError(final int errorCode, final String errorMessage) {
    }

    @Override
    public void waitCompletion() {
    }

    @Override
    public boolean waitCompletion(final long timeout) {
      return true;
    }

    @Override
    public void pageSyncLineUp() {
    }

    @Override
    public void pageSyncDone() {
    }
  }


  //////////////////
  // BELOW ARE NOT REQUIRED TO BE IMPLEMENTED
  //////////////////

  @Override
  protected void beforeStart() throws Exception {

  }

  /*
   * Methods Below may not need to be implemented
   */

  @Override
  protected LargeServerMessage parseLargeMessage(ActiveMQBuffer buff) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void performCachedLargeMessageDeletes() {
    //not supporting large messages
    LOGGER.debug("Unsupported method performCachedLargeMessageDeletes called");
  }

  @Override
  public LargeServerMessage createLargeMessage() {
    //not supported
    throw new UnsupportedOperationException();
  }

  @Override
  public LargeServerMessage createLargeMessage(long id, Message message) throws Exception {
    //not supported
    throw new ActiveMQIOErrorException("Message larger than max messag size");
  }

  @Override
  public LargeServerMessage largeMessageCreated(long id, LargeServerMessage largeMessage)
      throws Exception {
    //not supported
    throw new UnsupportedOperationException();
  }

  @Override
  public SequentialFile createFileForLargeMessage(long messageID, LargeMessageExtension extension) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteLargeMessageBody(LargeServerMessage largeServerMessage)
      throws ActiveMQException {
    LOGGER.debug("Unsupported method deleteLargeMessageBody called");
  }
}
