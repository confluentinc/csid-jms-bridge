/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public abstract class JournalStreamTransformer implements
    Transformer<
            JournalEntryKey,
            JournalEntry,
            Iterable<KeyValue<JournalEntryKey, JournalEntry>>> {

  private final String journalName;
  private final String storeName;
  private ProcessorContext context;
  private KeyValueStore<JournalEntryKey, ValueAndTimestamp<JournalEntry>> store;

  public JournalStreamTransformer(String journalName, String storeName) {
    this.journalName = journalName;
    this.storeName = storeName;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = (KeyValueStore<JournalEntryKey, ValueAndTimestamp<JournalEntry>>)
        context.getStateStore(this.storeName);
  }

  @Override
  public abstract Iterable<KeyValue<JournalEntryKey, JournalEntry>> transform(
      JournalEntryKey readOnlyKey, JournalEntry entry);

  @Override
  public void close() {
    //do nothing
  }

  public String getJournalName() {
    return journalName;
  }

  public String getStoreName() {
    return storeName;
  }

  protected ProcessorContext getContext() {
    return context;
  }

  protected KeyValueStore<JournalEntryKey, ValueAndTimestamp<JournalEntry>> getStore() {
    return store;
  }
}
