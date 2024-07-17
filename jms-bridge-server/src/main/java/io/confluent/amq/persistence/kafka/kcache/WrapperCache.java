//package io.confluent.amq.persistence.kafka.kcache;
//
//import com.google.protobuf.Message;
//import io.kcache.*;
//import io.kcache.exceptions.CacheInitializationException;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.header.Headers;
//import org.apache.kafka.common.serialization.Serde;
//
//import java.io.IOException;
//import java.util.*;
//import java.util.function.BiConsumer;
//import java.util.function.BiFunction;
//import java.util.function.Function;
//
//public class WrapperCache extends KafkaCache<CacheJournalEntryKey, CacheJournalEntry> {
//    private final KafkaCache<CacheJournalEntryKey, CacheJournalEntry> delegate;
//
//    public WrapperCache(String bootstrapServers) {
//        super(bootstrapServers, keySerde, valueSerde);
//        this.delegate = delegate;
//    }
//
//    public WrapperCache(KafkaCacheConfig config) {
//        super(config, keySerde, valueSerde);
//        this.delegate = delegate;
//    }
//
//    public WrapperCache(KafkaCacheConfig config, CacheUpdateHandler<CacheJournalEntryKey, CacheJournalEntry> cacheUpdateHandler) {
//        super(config, keySerde, valueSerde, cacheUpdateHandler, localCache);
//        this.delegate = delegate;
//    }
//
//    public WrapperCache(KafkaCacheConfig config, CacheUpdateHandler<CacheJournalEntryKey, CacheJournalEntry> cacheUpdateHandler, String backingCacheName, Comparator<K> comparator, KafkaCache<K, V> delegate) {
//        super(config, keySerde, valueSerde, cacheUpdateHandler, backingCacheName, comparator);
//        this.delegate = delegate;
//    }
//
//    @Override
//    public Comparator<? super K> comparator() {
//        return delegate.comparator();
//    }
//
//    @Override
//    public boolean isPersistent() {
//        return delegate.isPersistent();
//    }
//
//    @Override
//    public void init() throws CacheInitializationException {
//        delegate.init();
//    }
//
//    @Override
//    public void reset() {
//        delegate.reset();
//    }
//
//    @Override
//    public void sync() {
//        delegate.sync();
//    }
//
//    @Override
//    public int size() {
//        return delegate.size();
//    }
//
//    @Override
//    public boolean isEmpty() {
//        return delegate.isEmpty();
//    }
//
//    @Override
//    public boolean containsKey(Object key) {
//        return delegate.containsKey(key);
//    }
//
//    @Override
//    public boolean containsValue(Object value) {
//        return delegate.containsValue(value);
//    }
//
//    @Override
//    public V get(Object key) {
//        return delegate.get(key);
//    }
//
//    @Override
//    public V put(K key, V value) {
//        return delegate.put(key, value);
//    }
//
//    @Override
//    public Metadata<V> put(Headers headers, K key, V value) {
//        return delegate.put(headers, key, value);
//    }
//
//    @Override
//    public Metadata<V> put(Headers headers, K key, V value, boolean flush) {
//        return delegate.put(headers, key, value, flush);
//    }
//
//    @Override
//    public void putAll(Map<? extends K, ? extends V> entries) {
//        delegate.putAll(entries);
//    }
//
//    @Override
//    public RecordMetadata putAll(Headers headers, Map<? extends K, ? extends V> entries) {
//        return delegate.putAll(headers, entries);
//    }
//
//    @Override
//    public RecordMetadata putAll(Headers headers, Map<? extends K, ? extends V> entries, boolean flush) {
//        return delegate.putAll(headers, entries, flush);
//    }
//
//    @Override
//    public V remove(Object key) {
//        return delegate.remove(key);
//    }
//
//    @Override
//    public void clear() {
//        delegate.clear();
//    }
//
//    @Override
//    public Set<K> keySet() {
//        return delegate.keySet();
//    }
//
//    @Override
//    public Collection<V> values() {
//        return delegate.values();
//    }
//
//    @Override
//    public Set<Entry<K, V>> entrySet() {
//        return delegate.entrySet();
//    }
//
//    @Override
//    public K firstKey() {
//        return delegate.firstKey();
//    }
//
//    @Override
//    public K lastKey() {
//        return delegate.lastKey();
//    }
//
//    @Override
//    public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
//        return delegate.subCache(from, fromInclusive, to, toInclusive);
//    }
//
//    @Override
//    public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive) {
//        return delegate.range(from, fromInclusive, to, toInclusive);
//    }
//
//    @Override
//    public KeyValueIterator<K, V> all() {
//        return delegate.all();
//    }
//
//    @Override
//    public Cache<K, V> descendingCache() {
//        return delegate.descendingCache();
//    }
//
//    @Override
//    public void flush() {
//        delegate.flush();
//    }
//
//    @Override
//    public void close() throws IOException {
//        delegate.close();
//    }
//
//    @Override
//    public void destroy() throws IOException {
//        delegate.destroy();
//    }
//
//    @Override
//    public void configure(Map<String, ?> configs) {
//        delegate.configure(configs);
//    }
//
//    @Override
//    public SortedMap<K, V> subMap(K fromKey, K toKey) {
//        return delegate.subMap(fromKey, toKey);
//    }
//
//    @Override
//    public SortedMap<K, V> headMap(K toKey) {
//        return delegate.headMap(toKey);
//    }
//
//    @Override
//    public SortedMap<K, V> tailMap(K fromKey) {
//        return delegate.tailMap(fromKey);
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        return delegate.equals(o);
//    }
//
//    @Override
//    public int hashCode() {
//        return delegate.hashCode();
//    }
//
//    @Override
//    public V getOrDefault(Object key, V defaultValue) {
//        return delegate.getOrDefault(key, defaultValue);
//    }
//
//    @Override
//    public void forEach(BiConsumer<? super K, ? super V> action) {
//        delegate.forEach(action);
//    }
//
//    @Override
//    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
//        delegate.replaceAll(function);
//    }
//
//    @Override
//    public V putIfAbsent(K key, V value) {
//        return delegate.putIfAbsent(key, value);
//    }
//
//    @Override
//    public boolean remove(Object key, Object value) {
//        return delegate.remove(key, value);
//    }
//
//    @Override
//    public boolean replace(K key, V oldValue, V newValue) {
//        return delegate.replace(key, oldValue, newValue);
//    }
//
//    @Override
//    public V replace(K key, V value) {
//        return delegate.replace(key, value);
//    }
//
//    @Override
//    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
//        return delegate.computeIfAbsent(key, mappingFunction);
//    }
//
//    @Override
//    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
//        return delegate.computeIfPresent(key, remappingFunction);
//    }
//
//    @Override
//    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
//        return delegate.compute(key, remappingFunction);
//    }
//
//    @Override
//    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
//        return delegate.merge(key, value, remappingFunction);
//    }
//
//    public static <K1, V1> Map<K1, V1> of() {
//        return Map.of();
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1) {
//        return Map.of(k1, v1);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2) {
//        return Map.of(k1, v1, k2, v2);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3) {
//        return Map.of(k1, v1, k2, v2, k3, v3);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5, K1 k6, V1 v6) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5, K1 k6, V1 v6, K1 k7, V1 v7) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5, K1 k6, V1 v6, K1 k7, V1 v7, K1 k8, V1 v8) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5, K1 k6, V1 v6, K1 k7, V1 v7, K1 k8, V1 v8, K1 k9, V1 v9) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
//    }
//
//    public static <K1, V1> Map<K1, V1> of(K1 k1, V1 v1, K1 k2, V1 v2, K1 k3, V1 v3, K1 k4, V1 v4, K1 k5, V1 v5, K1 k6, V1 v6, K1 k7, V1 v7, K1 k8, V1 v8, K1 k9, V1 v9, K1 k10, V1 v10) {
//        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
//    }
//
//    @SafeVarargs
//    public static <K1, V1> Map<K1, V1> ofEntries(Entry<? extends K1, ? extends V1>... entries) {
//        return Map.ofEntries(entries);
//    }
//
//    public static <K1, V1> Entry<K1, V1> entry(K1 k1, V1 v1) {
//        return Map.entry(k1, v1);
//    }
//
//    public static <K1, V1> Map<K1, V1> copyOf(Map<? extends K1, ? extends V1> map) {
//        return Map.copyOf(map);
//    }
//}
