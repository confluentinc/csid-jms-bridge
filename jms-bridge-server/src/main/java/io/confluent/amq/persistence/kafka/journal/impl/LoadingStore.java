/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public final class LoadingStore implements KeyValueStore<Bytes, byte[]> {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(LoadingStore.class));

  public static KeyValueBytesStoreSupplier wrap(KeyValueBytesStoreSupplier other) {
    return new LoadingStoreSupplier(other);
  }

  private final KeyValueStore<Bytes, byte[]> delegateStore;
  private final AtomicInteger openTx = new AtomicInteger(0);

  private LoadingStore(
      KeyValueStore<Bytes, byte[]> delegateStore) {

    this.delegateStore = delegateStore;
  }

  private JournalEntryKey parseKey(Bytes bytes) {
    if (bytes != null) {
      try {
        return JournalEntryKey.parseFrom(bytes.get());
      } catch (Exception e) {
        //can't parse, move on then
      }
    }
    return null;
  }

  @Override
  public void put(Bytes key, byte[] value) {
    JournalEntryKey entry = parseKey(key);
    if (entry != null) {
      if (entry.getTxId() != 0 && entry.getMessageId() == 0 && entry.getExtendedId() == 0) {
        if (value != null) {
          openTx.incrementAndGet();
        } else {
          openTx.decrementAndGet();
        }
      }
    }
    SLOG.info(b -> b.event("put")
        .addJournalEntryKey(entry)
        .putTokens("openTxCount", openTx.get())
        .putTokens("tombstone", value == null));
    delegateStore.put(key, value);
  }

  @Override
  public byte[] putIfAbsent(Bytes key, byte[] value) {
    SLOG.info(b -> b.event("putIfAbsent").putTokens("tombstone", value == null));
    return delegateStore.putIfAbsent(key, value);
  }

  @Override
  public void putAll(
      List<KeyValue<Bytes, byte[]>> entries) {
    delegateStore.putAll(entries);
  }

  @Override
  public byte[] delete(Bytes key) {
    SLOG.info(b -> b.event("delete"));
    return delegateStore.delete(key);
  }

  @Override
  public String name() {
    return delegateStore.name();
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    delegateStore.init(context, root);
  }

  @Override
  public void flush() {
    delegateStore.flush();
  }

  @Override
  public void close() {
    delegateStore.close();
  }

  @Override
  public boolean persistent() {
    return delegateStore.persistent();
  }

  @Override
  public boolean isOpen() {
    return delegateStore.isOpen();
  }

  @Override
  public byte[] get(Bytes key) {
    return delegateStore.get(key);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    return delegateStore.range(from, to);
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return delegateStore.all();
  }

  @Override
  public long approximateNumEntries() {
    return delegateStore.approximateNumEntries();
  }

  public static final class LoadingStoreSupplier implements KeyValueBytesStoreSupplier {
    private final KeyValueBytesStoreSupplier delegate;

    private LoadingStoreSupplier(KeyValueBytesStoreSupplier delegate) {
      this.delegate = delegate;
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
      return new LoadingStore(this.delegate.get());
    }

    @Override
    public String metricsScope() {
      return this.delegate.metricsScope();
    }
  }
}
