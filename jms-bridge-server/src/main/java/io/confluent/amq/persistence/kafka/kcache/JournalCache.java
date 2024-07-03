package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerConfig;

@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class JournalCache implements AutoCloseable {

  static final StructuredLogger SLOG =
      StructuredLogger.with(b -> b.loggerClass(JournalCache.class));

  static final String TOPIC_FORMAT = "_jms.bridge_%s_%s_%s";

  final String bridgeId;
  final BridgeClientId bridgeClientId;
  final AtomicBoolean initialized = new AtomicBoolean(false);
  final Map<String, String> bindingsCacheConfig;
  final Map<String, String> messagesCacheConfig;
  final WalResolver bindingsResolver;
  final WalResolver messagesResolver;

  // both contain wal with transactions
  volatile KafkaCache<JournalEntryKey, JournalEntry> bindingsCache;
  volatile KafkaCache<JournalEntryKey, JournalEntry> messagesCache;

  public JournalCache(
      String bridgeId,
      BridgeClientId bridgeClientId,
      Map<String, String> bindingsCacheConfig,
      Map<String, String> messagesCacheConfig) {
    this.bridgeId = bridgeId;
    this.bridgeClientId = bridgeClientId;
    this.bindingsCacheConfig = bindingsCacheConfig;
    this.messagesCacheConfig = messagesCacheConfig;
    this.bindingsResolver =
        new WalResolver(KafkaJournalStorageManager.BINDINGS_NAME, this::getBindingsCache);
    this.messagesResolver =
        new WalResolver(KafkaJournalStorageManager.MESSAGES_NAME, this::getMessagesCache);
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

  private KafkaCache<JournalEntryKey, JournalEntry> initBindingsCache(Map<String, String> configs) {
    String topic =
        configs.getOrDefault(
            KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
            String.format(TOPIC_FORMAT, bridgeId, "bindings", bridgeClientId, "cache"));
    return getJournalEntryKeyJournalEntryCache(configs, topic, bindingsResolver);
  }

  private KafkaCache<JournalEntryKey, JournalEntry> initMessagesCache(Map<String, String> configs) {
    String topic =
        configs.getOrDefault(
            KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
            String.format(TOPIC_FORMAT, bridgeId, "messages", bridgeClientId, "cache"));
    return getJournalEntryKeyJournalEntryCache(configs, topic, messagesResolver);
  }

  private KafkaCache<JournalEntryKey, JournalEntry> getJournalEntryKeyJournalEntryCache(
      Map<String, String> configs, String topic, WalResolver resolver) {
    String groupId =
        configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, bridgeId + "-" + topic);
    String clientId = configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId);

    KafkaCache<JournalEntryKey, JournalEntry> cache;
    Map<String, Object> cacheConfig = new HashMap<>(configs);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
    cacheConfig.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
    cacheConfig.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        JournalEntryKeyPartitioner.class.getCanonicalName());
    JournalCacheUpdateHandler updateHandler = new JournalCacheUpdateHandler(resolver);

    // local cache is used for sending in a custom cache.
    cache =
        new KafkaCache<>(
            new KafkaCacheConfig(configs),
            JournalKeySerde.DEFAULT,
            JournalValueSerde.DEFAULT,
            updateHandler,
            null);
    cache.init();
    return cache;
  }

  public KafkaCache<JournalEntryKey, JournalEntry> getMessagesCache() {
    return messagesCache;
  }

  public KafkaCache<JournalEntryKey, JournalEntry> getBindingsCache() {
    return bindingsCache;
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
