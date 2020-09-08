/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor.JournalSpec;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.MDC;

public class KafkaIntegration {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaIntegration.class));

  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;
  public static final int SEGMENT_MAX_BYTES = 100 * 1024 * 1024;
  public static final String HA_ROLE_KEY = "haRole";

  private final JmsBridgeConfiguration config;
  private final KafkaIO kafkaIO;
  private final KafkaJournalProcessor journalProcessor;
  private final KJournal bindingsJournal;
  private final KJournal messagesJournal;
  private final String bridgeId;
  private final String clientId;
  private final String applicationId;
  private final UUID nodeUuid;

  public KafkaIntegration(JmsBridgeConfiguration config) {
    updateLogDiagnosticContext(config);
    this.config = config;
    if (!config.getJmsBridgeProperties().containsKey("bridge.id")) {
      SLOG.error(
          b -> b.event("Init").markFailure().message("'bridge.id' is a required configuration"));

      throw new IllegalStateException("A bridge id is required for using the Kafka Journal");
    }

    nodeUuid = UUIDGenerator.getInstance().generateUUID();
    bridgeId = config.getJmsBridgeProperties().getProperty("bridge.id");
    clientId = String.format("%s_%s", bridgeId, nodeUuid.toString());
    applicationId = String.format("jms.bridge.%s", this.bridgeId);

    List<JournalSpec> jspecs = new LinkedList<>();
    jspecs.add(createProcessor(KafkaJournalStorageManager.BINDINGS_NAME, config));
    jspecs.add(createProcessor(KafkaJournalStorageManager.MESSAGES_NAME, config));
    journalProcessor = new KafkaJournalProcessor(
        jspecs,
        clientId,
        getEffectiveProcessorProps(config));
    kafkaIO = new KafkaIO(config.getJmsBridgeProperties());

    bindingsJournal = journalProcessor.getJournal(KafkaJournalStorageManager.BINDINGS_NAME);
    messagesJournal = journalProcessor.getJournal(KafkaJournalStorageManager.MESSAGES_NAME);
  }

  private void updateLogDiagnosticContext(JmsBridgeConfiguration config) {
    String haType =
        config.getHAPolicyConfiguration() == null
            ? "null"
            : config.getHAPolicyConfiguration().getType().toString();
    MDC.put(HA_ROLE_KEY, haType);
  }

  private JournalSpec createProcessor(
      String journalName, JmsBridgeConfiguration config) {

    return new JournalSpec.Builder()
        .journalName(journalName)
        .journalTopic(KafkaJournal.journalTopic(bridgeId, journalName))
        .build();
  }

  public Map<String, String> getEffectiveJournalTopicProps(JmsBridgeConfiguration config) {
    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    topicProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(SEGMENT_MAX_BYTES));
    topicProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Integer.toString(MAX_RECORD_SIZE + 1024));

    return topicProps;
  }

  public Properties getEffectiveProcessorProps(JmsBridgeConfiguration config) {

    Properties streamProps = config.getJmsBridgeProperties();
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
    streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    //requires 3 brokers at minimum
    //streamProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    return streamProps;
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
    for (KJournal j : this.journalProcessor.getJournals()) {
      kafkaIO.createTopicIfNotExists(
          j.topic(),
          1,
          1,
          getEffectiveJournalTopicProps(config));
    }
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

