/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.KJournal;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

  @SuppressWarnings("unchecked")
  @Override
  public void init(ProcessorContext context) {
    this.store = (KeyValueStore<JournalEntryKey, JournalEntry>) context
        .getStateStore(storeName);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void process(JournalEntryKey key, JournalEntry value) {
    if (value != null) {

      doUpsert(key, value).forEach(kv -> {
        SLOG.trace(b -> b
            .name(journal.name())
            .event("UpsertRecord")
            .addJournalEntryKey(kv.key)
            .addJournalEntry(kv.value));
        this.store.put(kv.key, kv.value);
      });

    } else {

      doDelete(key).forEach(k -> {
        SLOG.trace(b -> b
            .name(journal.name())
            .event("DeleteRecord")
            .addJournalEntryKey(k));
        this.store.delete(k);
      });
    }
  }

  public List<JournalEntryKey> doDelete(JournalEntryKey key) {

    List<JournalEntryKey> results = new LinkedList<>();

    if (KafkaRecordUtils.isAnnotationsKey(key)) {
      //need to delete all associated annotation records
      JournalEntry annRefVal = this.store.get(key);
      if (annRefVal != null && annRefVal.hasAnnotationReference()) {
        results.addAll(annRefVal.getAnnotationReference().getEntryReferencesList());
      }
    }

    //delete the original
    results.add(key);
    return results;
  }

  public List<KeyValue<JournalEntryKey, JournalEntry>> doUpsert(
      JournalEntryKey key, JournalEntry value) {

    if (value.hasEpochEvent()) {
      //don't add these to the store, they are for loading only
      journal.loader().maybeComplete(value.getEpochEvent());
      return Collections.emptyList();
    }

    List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();

    //need to update the annotation reference if this is what it is
    if (value.hasAnnotationReference()) {
      JournalEntry annRefVal = this.store.get(key);

      if (annRefVal != null && annRefVal.hasAnnotationReference()) {
        JournalEntry updatedAnnRefVal = JournalEntry.newBuilder(annRefVal)
            .setAnnotationReference(AnnotationReference
                .newBuilder(annRefVal.getAnnotationReference())
                .addAllEntryReferences(value.getAnnotationReference().getEntryReferencesList()))
            .build();

        results.add(KeyValue.pair(key, updatedAnnRefVal));
      } else {
        //first annotation reference
        results.add(KeyValue.pair(key, value));
      }
    } else {
      //just add it to the store as is
      results.add(KeyValue.pair(key, value));
    }

    return results;
  }


  @Override
  public void close() {

  }
}
