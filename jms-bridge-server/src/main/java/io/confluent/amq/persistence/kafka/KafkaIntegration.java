/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.journal.JournalSpec;
import io.confluent.amq.persistence.kafka.kcache.JournalCache;
import io.confluent.amq.persistence.kafka.kcache.KCacheJournalProcessor;

import java.io.IOException;
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
    private final KCacheJournalProcessor journalProcessor;
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
                        config.journals().bindings().kcache(),
                        false));
        jspecs.add(
                createProcessor(
                        KafkaJournalStorageManager.MESSAGES_NAME,
                        config.journals().messages().kcache(),
                        true));

        final BridgeClientId clientId = config.clientId().withEvenMoreClientId(nodeUuid.toString());
        this.kafkaIO = new KafkaIO(clientId, BridgeConfigFactory.mapToProps(config.kafka()));
        this.journalProcessor = new KCacheJournalProcessor(
                bridgeId,
                jspecs,
                clientId,
                config.journals().readyTimeout(),
                this.kafkaIO);
    }

  private JournalSpec createProcessor(
      String journalName, Map<String, String> kcacheConfig, boolean performRouting) {
    return new JournalSpec.Builder()
        .journalName(journalName)
        .putAllKcacheConfig(kcacheConfig)
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

    /**
     * Will start both the KafkaIo and journal processors.
     */
    public synchronized void start() throws Exception {
        this.kafkaIO.start();
        this.journalProcessor.start();

        SLOG.info(
                b -> b.event("Starting").markSuccess());
    }

    public synchronized void stop() throws Exception {
        doStop();
    }

    public KafkaIO getKafkaIO() {
        return kafkaIO;
    }

    public JournalCache getBindingsJournal() {
        return journalProcessor.getJournal(KafkaJournalStorageManager.BINDINGS_NAME);
    }

    public JournalCache getMessagesJournal() {
        return journalProcessor.getJournal(KafkaJournalStorageManager.MESSAGES_NAME);
    }

    private void doStop() throws IOException {
        SLOG.info(b -> b.event("Stopping"));
        this.journalProcessor.stop();
        this.kafkaIO.stop();
    }
}

