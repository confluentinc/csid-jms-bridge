/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.jboss.logging.Logger;

public class KafkaJournalStorageManager extends JournalStorageManager {

  private static final Logger logger = Logger.getLogger(KafkaJournalStorageManager.class);

  private final Properties kafkaProps;
  private KafkaIO kafkaIO;

  public KafkaJournalStorageManager(Properties kafkaProps, Configuration config,
      CriticalAnalyzer analyzer,
      ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutors) {
    super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors);
    this.kafkaProps = kafkaProps;
  }

  public KafkaJournalStorageManager(Properties kafkaProps, Configuration config,
      CriticalAnalyzer analyzer,
      ExecutorFactory executorFactory, ExecutorFactory ioExecutors) {
    super(config, analyzer, executorFactory, ioExecutors);
    this.kafkaProps = kafkaProps;
  }

  public KafkaJournalStorageManager(Properties kafkaProps, Configuration config,
      CriticalAnalyzer analyzer,
      ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutorService,
      ExecutorFactory ioExecutors, IOCriticalErrorListener criticalErrorListener) {
    super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors,
        criticalErrorListener);
    this.kafkaProps = kafkaProps;
  }

  public KafkaJournalStorageManager(Properties kafkaProps, Configuration config,
      CriticalAnalyzer analyzer,
      ExecutorFactory executorFactory, ExecutorFactory ioExecutors,
      IOCriticalErrorListener criticalErrorListener) {
    super(config, analyzer, executorFactory, ioExecutors, criticalErrorListener);
    this.kafkaProps = kafkaProps;
  }

  @Override
  public synchronized void start() throws Exception {
    super.start();
    kafkaIO = new KafkaIO(kafkaProps);
    kafkaIO.start();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (kafkaIO != null) {
      kafkaIO.stop();
    }
  }

  @Override
  public void storeMessage(Message message) throws Exception {
    ICoreMessage coreMessage = message.toCore();
    if (!KafkaIO.isKafkaMessage(coreMessage)) {
      message.toCore().setAnnotation(SimpleString.toSimpleString("KAFKA_REF"), "TBD");
      KafkaIO.KafkaRef kafkaRef = kafkaIO.writeMessage(message).get();
      message.toCore().setAnnotation(SimpleString.toSimpleString("KAFKA_REF"), kafkaRef.asString());
    }
    super.storeMessage(coreMessage);
  }


  @Override
  public JournalLoadInformation loadMessageJournal(PostOffice postOffice,
      PagingManager pagingManager, ResourceManager resourceManager,
      Map<Long, QueueBindingInfo> queueInfos,
      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
      Set<Pair<Long, Long>> pendingLargeMessages, List<PageCountPending> pendingNonTXPageCounter,
      JournalLoader journalLoader) throws Exception {

    final JournalLoadInformation loadInfo = super.loadMessageJournal(postOffice, pagingManager,
        resourceManager, queueInfos, duplicateIDMap, pendingLargeMessages,
        pendingNonTXPageCounter, journalLoader);

    logger.info("###### LoadMessageJournal completed.");
    String delim = "    " + System.lineSeparator();
    String addresses = postOffice.getAddresses().stream().map(SimpleString::toString)
        .collect(Collectors.joining(delim));
    logger.info("###### Known Addresses: " + delim + addresses);

    String queues = queueInfos.values().stream()
        .map(qb -> qb.getAddress() + "::" + qb.getQueueName()).collect(Collectors.joining(delim));
    logger.info("###### Known Queues: " + delim + queues);

    return loadInfo;
  }

  @Override
  public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos,
      List<GroupingInfo> groupingInfos, List<AddressBindingInfo> addressBindingInfos)
      throws Exception {
    return super.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);
  }

  @Override
  public JournalLoadInformation[] loadInternalOnly() throws Exception {
    return super.loadInternalOnly();
  }


}
