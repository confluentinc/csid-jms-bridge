package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.DelegatingConfluentAmqServer;
import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerConfig;

@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class JournalCache implements AutoCloseable {
    static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(JournalCache.class));

    static final String TOPIC_FORMAT = "_jms.bridge_%s_%s_%s";

    final String bridgeId;
    final BridgeClientId bridgeClientId;
    final AtomicBoolean initialized = new AtomicBoolean(false);
    final Map<String, String> cacheConfig;
    final WalResolver resolver;

    final CompletableFuture<Void> loadCompletion = new CompletableFuture<>();

    final String journalName;

    JournalCacheUpdateHandler updateHandler;

    // both contain wal with transactions
    volatile KafkaCache<JournalEntryKey, JournalEntry> cache;

    public JournalCache(
            String bridgeId,
            String journalName,
            BridgeClientId bridgeClientId,
            Map<String, String> cacheConfig) {

        this.journalName = journalName;
        this.bridgeId = bridgeId;
        this.bridgeClientId = bridgeClientId;
        this.cacheConfig = cacheConfig;
        this.resolver = new WalResolver(journalName, this::getCache);
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public synchronized void start() {
        SLOG.info(b -> b
                .name(journalName)
                .event("start"));

        initCache(cacheConfig);

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new IllegalStateException(
                    "Illegal state while initializing KafkaJournal. Journal already initialized.");
        }
    }

    public List<AnnotationReference> getAnnotationRefs() {
        return this.resolver.getAnnotationRefs();
    }

    public List<TransactionReference> getTransactionRefs() {
        return this.resolver.getTransactionRefs();
    }

    public synchronized void stop() throws IOException {
        SLOG.info(b -> b
                .name(journalName)
                .event("stop")
                .markStarted());

        if (cache != null) {
            cache.flush();
            cache.destroy();
        }

        SLOG.info(b -> b
                .name(journalName)
                .event("stop")
                .markCompleted());
    }

    public CompletableFuture<Void> onLoadComplete() {
        return loadCompletion;
    }

    public synchronized boolean isLeader() {
        // TODO: should use kafka consumer group leader election.
        // isInitialized() && elector.isLeader();
        return isInitialized();
    }

    private void initCache(Map<String, String> configs) {
        String topic =
                configs.getOrDefault(
                        KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
                        String.format(TOPIC_FORMAT, bridgeId, journalName, "_cache"));
        this.cache = getJournalEntryKeyJournalEntryCache(configs, topic, resolver);
        this.resolver.startReadOnlyProcessing();
        SLOG.info(b -> b
                .name(journalName)
                .event("start")
                .markCompleted());
    }

    private KafkaCache<JournalEntryKey, JournalEntry> getJournalEntryKeyJournalEntryCache(
            Map<String, String> configs, String topic, WalResolver resolver) {
        String groupId =
                configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, bridgeId + "-" + topic);

        KafkaCache<JournalEntryKey, JournalEntry> cache;
        Map<String, Object> cacheConfig = new HashMap<>(configs);
        cacheConfig.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        cacheConfig.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        cacheConfig.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, bridgeClientId);
        cacheConfig.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                JournalEntryKeyPartitioner.class.getCanonicalName());
        updateHandler = new JournalCacheUpdateHandler(journalName, resolver);

        updateHandler.onInitialized().whenComplete((i, e) -> {
            loadCompletion.complete(null);
            SLOG.info(b -> b
                    .name(journalName)
                    .event("load")
                    .markCompleted());
        });

        // local cache is used for sending in a custom cache.
        cache = new KafkaCache<>(
                        new KafkaCacheConfig(configs),
                        JournalKeySerde.DEFAULT,
                        JournalValueSerde.DEFAULT,
                        updateHandler,
                        null,
                        this::compareKeys);
        cache.init();

        return cache;
    }

    private int compareKeys(JournalEntryKey key1, JournalEntryKey key2) {
        if (key1 == null && key2 == null) {
            return 0;
        }

        if (key1 != null && key2 == null) {
            return 1;
        }

        if (key1 == null && key2 != null) {
            return -1;
        }

        return Long.compare(key1.getMessageId(), key2.getMessageId());
    }

    public KafkaCache<JournalEntryKey, JournalEntry> getCache() {
        return cache;
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            SLOG.info(b -> b
                    .name(journalName)
                    .event("close")
                    .markStarted());

            cache.close();

            SLOG.info(b -> b
                    .name(journalName)
                    .event("close")
                    .markCompleted());
        }
    }

    public String walTopic() {
        return String.format(
                TOPIC_FORMAT,
                bridgeId.toLowerCase(),
                journalName.toLowerCase(),
                "wal");
    }
}
