package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.JournalSpec;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.utils.Caches;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class KafkaJournal implements AutoCloseable {

  static final StructuredLogger SLOG =
      StructuredLogger.with(b -> b.loggerClass(KafkaJournal.class));

  static final String TOPIC_FORMAT = "_jms.bridge_%s_%s_%s";

  final String bridgeId;
  final BridgeClientId bridgeClientId;
  final KafkaIO kafkaIO;
  final List<JournalSpec> journalSpecs;
  final Duration loadTimeout; // maybe not needed
  final AtomicBoolean initialized = new AtomicBoolean(false);
  final Map<String, String> bindingsCacheConfig;
  final Map<String, String> messagesCacheConfig;

  // both contain wal with transactions
  volatile Cache<JournalEntryKey, JournalEntry> bindingsCache;
  volatile Cache<JournalEntryKey, JournalEntry> messagesCache;

  public KafkaJournal(
      String bridgeId,
      List<JournalSpec> journalSpecs,
      BridgeClientId bridgeClientId,
      Duration loadTimeout,
      Map<String, String> bindingsCacheConfig,
      Map<String, String> messagesCacheConfig,
      KafkaIO kafkaIO) {
    this.bridgeId = bridgeId;
    this.bridgeClientId = bridgeClientId;
    this.kafkaIO = kafkaIO;
    this.journalSpecs = journalSpecs;
    this.loadTimeout = loadTimeout;
    this.bindingsCacheConfig = bindingsCacheConfig;
    this.messagesCacheConfig = messagesCacheConfig;
  }

  public boolean isInitialized() {
    return initialized.get();
  }

  public synchronized void start() {
    CompletableFuture<Void> bindingsFuture =
            CompletableFuture.runAsync(
                    () -> this.bindingsCache = initBindingsCache(bindingsCacheConfig));
    CompletableFuture<Void> messagesFuture =
            CompletableFuture.runAsync(
                    () -> this.messagesCache = initMessagesCache(messagesCacheConfig));
    CompletableFuture.allOf(bindingsFuture, messagesFuture).join();

    boolean isInitialized = initialized.compareAndSet(false, true);
    if (!isInitialized) {
      throw new IllegalStateException(
              "Illegal state while initializing KafkaJournal. Journal already initialized.");
    }
  }

  public synchronized void sync() {
    CompletableFuture<Void> bindingsFuture =
        CompletableFuture.runAsync(() -> bindingsCache.reset())
            .thenRunAsync(() -> bindingsCache.sync());
    CompletableFuture<Void> messagesFuture =
        CompletableFuture.runAsync(() -> messagesCache.reset())
            .thenRunAsync(() -> messagesCache.sync());
    CompletableFuture.allOf(bindingsFuture, messagesFuture).join();
  }

  public synchronized boolean isLeader() {
    // TODO: should use kafka consumer group leader election.
    // isInitialized() && elector.isLeader();
    return isInitialized();
  }

  private Cache<JournalEntryKey, JournalEntry> initBindingsCache(Map<String, String> configs) {
    String topic =
        configs.getOrDefault(
            KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
            String.format(TOPIC_FORMAT, bridgeId, "bindings", bridgeClientId, "cache"));
    return getJournalEntryKeyJournalEntryCache(configs, topic);
  }

  private Cache<JournalEntryKey, JournalEntry> initMessagesCache(Map<String, String> configs) {
    String topic =
        configs.getOrDefault(
            KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
            String.format(TOPIC_FORMAT, bridgeId, "messages", bridgeClientId, "cache"));
    return getJournalEntryKeyJournalEntryCache(configs, topic);
  }

  private Cache<JournalEntryKey, JournalEntry> getJournalEntryKeyJournalEntryCache(
      Map<String, String> configs, String topic) {
    String groupId = configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, bridgeId);
    String clientId =
        configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);

    Cache<JournalEntryKey, JournalEntry> cache;
    Map<String, Object> cacheConfig = new HashMap<>(configs);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
    cache =
        new KafkaCache<>(
            new KafkaCacheConfig(configs), JournalKeySerde.DEFAULT, JournalValueSerde.DEFAULT);
    cache = Caches.concurrentCache(cache);
    cache.init();
    return cache;
  }

  @Override
  public void close() throws Exception {
    if (bindingsCache != null) {
      bindingsCache.close();
    }
    if (messagesCache != null) {
      messagesCache.close();
    }
  }
}
