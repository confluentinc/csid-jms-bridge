/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfig.JournalConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor.JournalSpec;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.kafka.common.config.TopicConfig;

public class KafkaIntegration {

  public static String applicationId(String bridgeId) {
    return String.format("jms.bridge.%s", bridgeId);
  }

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaIntegration.class));

  private final BridgeConfig config;
  private final KafkaIO kafkaIO;
  private final KafkaJournalProcessor journalProcessor;
  private final KJournal bindingsJournal;
  private final KJournal messagesJournal;
  private final String bridgeId;
  private final UUID nodeUuid;
  private final String applicationId;

  public KafkaIntegration(JmsBridgeConfiguration jmsConfig) {
    this.config = jmsConfig.getBridgeConfig();

    nodeUuid = UUIDGenerator.getInstance().generateUUID();
    bridgeId = config.id();
    applicationId = applicationId(bridgeId);

    List<JournalSpec> jspecs = new LinkedList<>();
    jspecs.add(
        createProcessor(
            KafkaJournalStorageManager.BINDINGS_NAME,
            config.journals().bindings(),
            false));
    jspecs.add(
        createProcessor(
            KafkaJournalStorageManager.MESSAGES_NAME,
            config.journals().messages(),
            true));

    this.kafkaIO = new KafkaIO(BridgeConfigFactory.mapToProps(config.kafka()));
    this.kafkaIO.start();

    final String clientId = String.format("%s_%s", bridgeId, nodeUuid.toString());
    this.journalProcessor = new KafkaJournalProcessor(
        jspecs,
        clientId,
        applicationId,
        this.config.streams(),
        this.kafkaIO);

    bindingsJournal = journalProcessor.getJournal(KafkaJournalStorageManager.BINDINGS_NAME);
    messagesJournal = journalProcessor.getJournal(KafkaJournalStorageManager.MESSAGES_NAME);
  }

  private JournalSpec createProcessor(
      String journalName, JournalConfig jconfig, boolean performRouting) {

    return new JournalSpec.Builder()
        .journalName(journalName)

        .mutateJournalWalTopic(wt -> wt
            .partitions(jconfig.walTopic().partitions())
            .replication(jconfig.walTopic().replication())
            .name(jconfig.walTopic().name()
                .orElse(KafkaJournal.journalWalTopic(bridgeId, journalName)))
            .putAllConfigs(getEffectiveJournalWalTopicProps(jconfig.walTopic().options())))

        .mutateJournalTableTopic(tt -> tt
            .partitions(jconfig.tableTopic().partitions())
            .replication(jconfig.tableTopic().replication())
            .name(jconfig.tableTopic().name()
                .orElse(KafkaJournal.journalTableTopic(bridgeId, journalName)))
            .putAllConfigs(getEffectiveJournalTableTopicProps(jconfig.tableTopic().options())))

        .performRouting(performRouting)
        .build();
  }

  public Map<String, String> getEffectiveJournalWalTopicProps(
      Map<String, Object> configTopicProps) {

    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    configTopicProps.forEach((k, v) -> topicProps.put(k, v.toString()));

    return topicProps;
  }

  public Map<String, String> getEffectiveJournalTableTopicProps(
      Map<String, Object> configTopicProps) {

    Map<String, String> topicProps = new HashMap<>();
    configTopicProps.forEach((k, v) -> topicProps.put(k, v.toString()));
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

    return topicProps;
  }

  public String getBridgeId() {
    return bridgeId;
  }

  public UUID getNodeUuid() {
    return nodeUuid;
  }

  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Starts the basic kafka clients as part of the KafkaIO class. This method may be called multiple
   * times without repercussion.
   */
  public synchronized void startKafkaIo() {
    kafkaIO.start();
  }

  /**
   * Will start both the KafkaIo and journal processors.
   */
  public synchronized void start() throws Exception {
    this.journalProcessor.start();
    SLOG.info(
        b -> b.event("Starting").markSuccess());
  }

  public synchronized void stop() throws Exception {
    doStop();
  }

  public synchronized void stopProcessor() throws Exception {
    SLOG.info(b -> b.event("StoppingProcessor"));
    this.journalProcessor.stop();
  }

  public void waitForProcessorRunning() throws Exception {
    while (!journalProcessor.isRunning()) {
      Thread.sleep(100);
    }
  }

  public void waitForProcessorObtainPartition() throws Exception {
    while (true) {
      if (this.bindingsJournal.isAssignedPartition(0)) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
  }

  public void waitForProcessorReleasePartition() throws Exception {
    while (true) {
      if (!this.bindingsJournal.isAssignedPartition(0)) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
  }

  public KafkaIO getKafkaIO() {
    return kafkaIO;
  }

  public KJournal getBindingsJournal() {
    return bindingsJournal;
  }

  public KJournal getMessagesJournal() {
    return messagesJournal;
  }

  private void doStop() {
    SLOG.info(b -> b.event("Stopping"));
    this.journalProcessor.stop();
    this.kafkaIO.stop();
  }
}

