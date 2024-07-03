/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.kcache.JournalCache;
import io.confluent.amq.persistence.kafka.kcache.KafkaCacheJournal;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIOErrorException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.ArtemisCloseable;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

public class KafkaJournalStorageManager extends JournalStorageManager {

  public static final String BINDINGS_NAME = "bindings";
  public static final String MESSAGES_NAME = "messages";

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaJournalStorageManager.class));


  private KafkaIO kafkaIO;
  private JournalCache journalCache;
  private KafkaIntegration kafkaIntegration;

  @Override
  public ArtemisCloseable closeableReadLock() {
    return super.closeableReadLock();
  }

  public KafkaJournalStorageManager(
      KafkaIntegration kafkaIntegration,
      JmsBridgeConfiguration config,
      CriticalAnalyzer analyzer, ExecutorFactory executorFactory,
      ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutorFactory) {

    this(kafkaIntegration, config, analyzer, executorFactory,
        scheduledExecutorService, ioExecutorFactory,
        null);
  }

  public KafkaJournalStorageManager(
      KafkaIntegration kafkaIntegration,
      JmsBridgeConfiguration config,
      CriticalAnalyzer analyzer, ExecutorFactory executorFactory,
      ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutorFactory,
      IOCriticalErrorListener criticalErrorListener) {

    super(new InitWorkAroundWrapper(config, kafkaIntegration),
        analyzer,
        executorFactory,
        scheduledExecutorService,
        ioExecutorFactory,
        criticalErrorListener);
  }

  @Override
  protected synchronized void init(Configuration config,
      IOCriticalErrorListener criticalErrorListener) {
    SLOG.info(b -> b.event("Init"));

    InitWorkAroundWrapper jbConfig = (InitWorkAroundWrapper) config;

    this.kafkaIntegration = jbConfig.kafkaIntegration;

    this.kafkaIO = this.kafkaIntegration.getKafkaIO();
    this.journalCache = this.kafkaIntegration.getJournalCache();

    this.messageJournal =
        new KafkaCacheJournal(
            MESSAGES_NAME, journalCache.getMessagesCache(), this.ioCriticalErrorListener);
    this.bindingsJournal =
        new KafkaCacheJournal(
            BINDINGS_NAME, journalCache.getBindingsCache(), this.ioCriticalErrorListener);

    SLOG.info(b -> b.event("Init").markSuccess());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
  @Override
  public void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
    if (!started) {
      return;
    }

    if (!ioCriticalError) {
      //performCachedLargeMessageDeletes();
      // Must call close to make sure last id is persisted
      if (journalLoaded && idGenerator != null) {
        idGenerator.persistCurrentID();
      }
    }

    final CountDownLatch latch = new CountDownLatch(1);
    executorFactory.getExecutor().execute(latch::countDown);

    latch.await(30, TimeUnit.SECONDS);

    beforeStop();

    bindingsJournal.stop();
    messageJournal.stop();
    //largeMessagesFactory.stop();

    journalLoaded = false;

    started = false;
    SLOG.info(b -> b.event("Stop"));
  }

  @Override
  public long generateID() {
    return super.generateID() + 1;
  }

  @Override
  public ByteBuffer allocateDirectBuffer(int size) {
    return NIOSequentialFileFactory.allocateDirectByteBuffer(size);
  }

  @Override
  public void freeDirectBuffer(ByteBuffer buffer) {
    //nop
  }

  //////////////////
  // BELOW ARE NOT REQUIRED TO BE IMPLEMENTED
  //////////////////

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
    SLOG.debug(
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
    SLOG.debug(
        b -> b.event("UnsupportedOperationCalled").message("deleteLargeMessageBody"));
  }

  @Override
  public void updateQueueBinding(long tx, Binding binding) throws Exception {
    super.updateQueueBinding(tx, binding);
    SLOG.info(b -> b
        .event("UpdateQueueBinding")
        .putTokens("binding", binding.toManagementString())
        .putTokens("bindingId", binding.getID())
        .putTokens("tx", tx)
    );
  }

  @Override
  public void addQueueBinding(long tx, Binding binding) throws Exception {
    super.addQueueBinding(tx, binding);
    SLOG.info(b -> b
        .event("AddQueueBinding")
        .putTokens("binding", binding.toManagementString())
        .putTokens("bindingId", binding.getID())
        .putTokens("tx", tx)
    );
  }

  @Override
  public void deleteQueueBinding(long tx, long queueBindingID) throws Exception {
    super.deleteQueueBinding(tx, queueBindingID);
    SLOG.info(b -> b
        .event("DeleteQueueBinding")
        .putTokens("queueBindingId", queueBindingID)
        .putTokens("tx", tx)
    );
  }

  @Override
  public void addAddressBinding(long tx, AddressInfo addressInfo) throws Exception {
    super.addAddressBinding(tx, addressInfo);
    SLOG.info(b -> b
        .event("AddAddressBinding")
        .putTokens("addressName", addressInfo.getName())
        .putTokens("addressId", addressInfo.getId())
        .putTokens("tx", tx)
    );
  }

  @Override
  public void deleteAddressBinding(long tx, long addressBindingID) throws Exception {
    super.deleteAddressBinding(tx, addressBindingID);
    SLOG.info(b -> b
        .event("DeleteAddressBinding")
        .putTokens("addressId", addressBindingID)
        .putTokens("tx", tx)
    );
  }

  static class InitWorkAroundWrapper extends JmsBridgeConfiguration {

    final KafkaIntegration kafkaIntegration;

    InitWorkAroundWrapper(
        JmsBridgeConfiguration configuration,
        KafkaIntegration kafkaIntegration) {

      super(configuration.getDelegate(), configuration.getBridgeConfig());
      this.kafkaIntegration = kafkaIntegration;
    }
  }
}

