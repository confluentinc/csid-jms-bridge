/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class GlobalStoreProcessor implements Processor<JournalEntryKey, JournalEntry> {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(GlobalStoreProcessor.class));

  private final String storeName;
  private final KJournal journal;

  private KeyValueStore<JournalEntryKey, JournalEntry> store;

  public GlobalStoreProcessor(String storeName,
      KJournal journal) {
    this.storeName = storeName;
    this.journal = journal;
  }

  @Override
  public void init(ProcessorContext context) {
    this.store = (KeyValueStore<JournalEntryKey, JournalEntry>) context
        .getStateStore(storeName);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void process(JournalEntryKey key, JournalEntry value) {

    if (value != null && value.hasEpochEvent()) {
      journal.loader().maybeComplete(value.getEpochEvent());
    } else {

      if (value != null) {
        SLOG.trace(b -> b
            .name(journal.name())
            .event("UpsertRecord")
            .addJournalEntryKey(key)
            .addJournalEntry(value));
        this.store.put(key, value);
      } else {
        SLOG.trace(b -> b
            .name(journal.name())
            .event("DeleteRecord")
            .addJournalEntryKey(key));
        this.store.delete(key);
      }
    }
  }

  @Override
  public void close() {

  }
}
