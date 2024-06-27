package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.TransactionReference;
import io.kcache.CacheUpdateHandler;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JournalCacheUpdateHandler implements CacheUpdateHandler<JournalEntryKey, JournalEntry> {
    ConcurrentHashMap<String, TransactionReference> txCache = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, AnnotationReference> annCache = new ConcurrentHashMap<>();

    @Override
    public void cacheSynchronized(int count, Map<TopicPartition, Long> checkpoints) {
        CacheUpdateHandler.super.cacheSynchronized(count, checkpoints);
    }

    @Override
    public void cacheInitialized(int count, Map<TopicPartition, Long> checkpoints) {
        CacheUpdateHandler.super.cacheInitialized(count, checkpoints);
    }

    @Override
    public void handleUpdate(JournalEntryKey key, JournalEntry value, JournalEntry oldValue, TopicPartition tp, long offset, long ts) {
        // Transaction handling
        //open transaction
        //append to transaction
        //close transaction (commit,rollback)


        // Annotation Handling
        // add message
        // annotate message
        // delete message
    }
}
