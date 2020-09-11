/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfig.JournalConfig;
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
    applicationId = String.format("jms.bridge.%s", this.bridgeId);
    String clientId = String.format("%s_%s", bridgeId, nodeUuid.toString());

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

    journalProcessor = new KafkaJournalProcessor(
        jspecs,
        clientId,
        applicationId,
        this.config);

    kafkaIO = new KafkaIO(config.kafka());

    bindingsJournal = journalProcessor.getJournal(KafkaJournalStorageManager.BINDINGS_NAME);
    messagesJournal = journalProcessor.getJournal(KafkaJournalStorageManager.MESSAGES_NAME);
  }

  private JournalSpec createProcessor(
      String journalName, JournalConfig jconfig, boolean performRouting) {

    return new JournalSpec.Builder()
        .journalName(journalName)
        .journalTopic(
            jconfig.topic().name().orElse(KafkaJournal.journalTopic(bridgeId, journalName)))
        .performRouting(performRouting)
        .build();
  }

  public Map<String, String> getEffectiveJournalTopicProps(Map<String, Object> configTopicProps) {
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
   * Will create the necessary journal topics in kafka if needed.
   */
  public void createJournalTopics() {
    kafkaIO.createTopicIfNotExists(
        bindingsJournal.topic(),
        config.journals().bindings().topic().partitions(),
        config.journals().bindings().topic().replication(),
        getEffectiveJournalTopicProps(config.journals().bindings().topic().options()));

    kafkaIO.createTopicIfNotExists(
        messagesJournal.topic(),
        config.journals().messages().topic().partitions(),
        config.journals().messages().topic().replication(),
        getEffectiveJournalTopicProps(config.journals().messages().topic().options()));
  }

  /**
   * Will start both the KafkaIo and journal processors.
   */
  public synchronized void start() throws Exception {
    kafkaIO.start();
    createJournalTopics();

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

