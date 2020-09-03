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
import java.util.UUID;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaIntegration {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(KafkaIntegration.class));

  private static final Integer MAX_RECORD_SIZE = 1024 * 1024;
  public static final int SEGMENT_MAX_BYTES = 100 * 1024 * 1024;

  private final JmsBridgeConfiguration config;
  private final KafkaIO kafkaIO;
  private final KafkaJournalProcessor journalProcessor;
  private final KJournal bindingsJournal;
  private final KJournal messagesJournal;
  private final String bridgeId;
  private final String nodeId;

  public KafkaIntegration(JmsBridgeConfiguration config) {
    this.config = config;
    if (!config.getJmsBridgeProperties().containsKey("bridge.id")) {
      SLOG.error(
          b -> b.event("Init").markFailure().message("'bridge.id' is a required configuration"));

      throw new IllegalStateException("A bridge id is required for using the Kafka Journal");
    }

    nodeId = UUID.randomUUID().toString();
    bridgeId = config.getJmsBridgeProperties().getProperty("bridge.id");

    List<JournalSpec> jspecs = new LinkedList<>();
    jspecs.add(createProcessor(KafkaJournalStorageManager.BINDINGS_NAME, config));
    jspecs.add(createProcessor(KafkaJournalStorageManager.MESSAGES_NAME, config));
    journalProcessor = new KafkaJournalProcessor(
        jspecs,
        nodeId,
        getEffectiveProcessorProps(config));
    kafkaIO = new KafkaIO(config.getJmsBridgeProperties());

    bindingsJournal = journalProcessor.getJournal(KafkaJournalStorageManager.BINDINGS_NAME);
    messagesJournal = journalProcessor.getJournal(KafkaJournalStorageManager.MESSAGES_NAME);
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
    streamProps.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        String.format("jms.bridge.%s", this.bridgeId));

    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
    streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    //requires 3 brokers at minimum
    //streamProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    return streamProps;
  }


  public synchronized void start() throws Exception {
    kafkaIO.start();

    for (KJournal j : this.journalProcessor.getJournals()) {
      kafkaIO.createTopicIfNotExists(
          j.topic(),
          1,
          1,
          getEffectiveJournalTopicProps(config));
    }

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

