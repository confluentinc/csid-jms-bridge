package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import lombok.SneakyThrows;
import org.apache.kafka.common.TopicPartition;

import java.sql.Date;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class JournalCacheUpdateHandler implements CacheUpdateHandler<JournalEntryKey, JournalEntry> {

    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(JournalCacheUpdateHandler.class));
    private final String journalName;
    private CompletableFuture<Void> onInitializedFuture = new CompletableFuture<>();
    private WalResolver walResolver;

    public JournalCacheUpdateHandler(String journalName, WalResolver walResolver) {
        this.walResolver = walResolver;
        this.journalName = journalName;
    }

    public CompletableFuture<Void> onInitialized() {
        return onInitializedFuture;
    }


    /**
     * This method is called only after the {@link KafkaCache#sync()} is called. It is invoked once the sync has
     * completed which means that every offset at the time of sync has been read into the cache.
     */
    @Override
    public void cacheSynchronized(int count, Map<TopicPartition, Long> checkpoints) {
        SLOG.info(b -> b.event("cacheSynchronized").name(journalName).markCompleted());
        onInitializedFuture.complete(null);
        CacheUpdateHandler.super.cacheSynchronized(count, checkpoints);
    }

    /**
     * This method is called when the cache is started. It is called once and only when the latest offset at the time
     * of initialization is read into the cache.
     */
    @Override
    public void cacheInitialized(int count, Map<TopicPartition, Long> checkpoints) {
        SLOG.info(b -> b.event("cacheInitialized").name(journalName).markCompleted());
        onInitializedFuture.complete(null);
    }

    /**
     * This method is called for every kafka record that is added to the cache, this includes tombstones.
     */
    @SneakyThrows
    @Override
    public void handleUpdate(JournalEntryKey key, JournalEntry value, JournalEntry oldValue, TopicPartition tp, long offset, long ts) {
        walResolver.submitEntry(key, value, Date.from(Instant.ofEpochMilli(ts)));
    }
}
