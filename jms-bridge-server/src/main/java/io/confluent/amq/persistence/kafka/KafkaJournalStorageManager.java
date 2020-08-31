/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.journal.KJournalAssignment;
import io.confluent.amq.persistence.kafka.journal.KJournalListener;
import io.confluent.amq.persistence.kafka.journal.KJournalMetadata;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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

public class KafkaJournalStorageManager extends JournalStorageManager {

  public static final String BINDINGS_NAME = "bindings";
  public static final String MESSAGES_NAME = "messages";

  private static final StructuredLogger LOGGER = StructuredLogger.with(b -> b
      .loggerClass(KafkaJournalStorageManager.class));


  private BroadcastListener broadcastListener;
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

  public void registerListener(KJournalListener journalListener) {
    this.broadcastListener.listeners.add(journalListener);
  }

  @Override
  protected synchronized void init(Configuration config,
      IOCriticalErrorListener criticalErrorListener) {
    //need to create these journals
    LOGGER.info(b -> b.event("Init"));

    this.broadcastListener = new BroadcastListener();
    JmsBridgeConfiguration jbConfig = (JmsBridgeConfiguration) config;

    if (!jbConfig.getJmsBridgeProperties().containsKey("bridge.id")) {
      LOGGER.error(
          b -> b.event("Init").markFailure().message("'bridge.id' is a required configuration"));

      throw new IllegalStateException("A bridge id is required for using the Kafka Journal");
    }
    String bridgeId = jbConfig.getJmsBridgeProperties().getProperty("bridge.id");

    this.kafkaIO = new KafkaIO(jbConfig.getJmsBridgeProperties());
    this.kafkaIO.start();

    this.bindingsJournal = new KafkaJournal(kafkaIO, bridgeId, jbConfig.getNodeId(), BINDINGS_NAME,
        executorFactory, criticalErrorListener, broadcastListener);
    this.messageJournal = new KafkaJournal(kafkaIO, bridgeId, jbConfig.getNodeId(), MESSAGES_NAME,
        executorFactory, criticalErrorListener, broadcastListener);

    LOGGER.info(b -> b.event("Init").markSuccess());
  }

  @Override
  public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
    LOGGER.info(b -> b.event("Stop"));

    super.stop(ioCriticalError, sendFailover);
    this.kafkaIO.stop();

    LOGGER.info(b -> b.event("Stop").markSuccess());
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
    LOGGER.debug(
        b -> b.event("UnsupportedOperationCalled").message("performCachedLargeMessageDeletes"));
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
    LOGGER.debug(
        b -> b.event("UnsupportedOperationCalled").message("deleteLargeMessageBody"));
  }

  static class BroadcastListener implements KJournalListener {

    private final List<KJournalListener> listeners = new LinkedList<>();

    @Override
    public void onAssignmentChange(KJournalMetadata metadata,
        List<KJournalAssignment> newAssignmentList) {

      for (KJournalListener listener : listeners) {
        try {
          listener.onAssignmentChange(metadata, newAssignmentList);
        } catch (Throwable t) {
          //
        }
      }
    }

    @Override
    public void onStateChange(KJournalMetadata metadata, KJournalState oldState,
        KJournalState newState) {

      for (KJournalListener listener : listeners) {
        try {
          listener.onStateChange(metadata, oldState, newState);
        } catch (Throwable t) {
          //
        }
      }
    }
  }
}
