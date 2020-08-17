/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.JournalStreamTransformer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MainRecordProcessor extends JournalStreamTransformer {

  private static final EnumSet<JournalRecordType> RECORDS_OF_INTEREST = EnumSet.of(
      JournalRecordType.DELETE_RECORD,
      JournalRecordType.ANNOTATE_RECORD
  );

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(MainRecordProcessor.class));

  public MainRecordProcessor(String journalName, String storeName) {
    super(journalName, storeName);
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> handleDelete(
      JournalEntryKey key, JournalEntry value) {

    if (value != null
        && value.getAppendedRecord().getRecordType() == JournalRecordType.DELETE_RECORD) {

      return Collections.singletonList(
          KeyValue.pair(
              KafkaRecordUtils.annotationsKeyFromRecordKey(key),
              null)
      );
    }

    return Collections.emptyList();
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> handleTombstones(
      JournalEntryKey key, JournalEntry value) {

    if (value == null) {
      return Collections.singletonList(
          KeyValue.pair(
              JournalEntryKey.newBuilder(key).build(),
              null
          )
      );
    }

    return Collections.emptyList();
  }

  @Override
  public Iterable<KeyValue<JournalEntryKey, JournalEntry>> transform(
      JournalEntryKey key, JournalEntry value) {

    if (value == null
        || !value.hasAppendedRecord()
        || !RECORDS_OF_INTEREST.contains(value.getAppendedRecord().getRecordType())) {
      //pass through
      return Collections.singletonList(KeyValue.pair(key, value));
    }

    List<KeyValue<JournalEntryKey, JournalEntry>> results = Collections.emptyList();
    JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);
    ValueAndTimestamp<JournalEntry> annStoreValue = getStore().get(annKey);

    long timestamp = getContext().timestamp();
    AnnotationReference annRef;
    if (annStoreValue != null) {
      annRef = annStoreValue.value().getAnnotationReference();
    } else {
      annRef = AnnotationReference.getDefaultInstance();
    }

    if (value.getAppendedRecord().getRecordType() == JournalRecordType.DELETE_RECORD) {
      results = new LinkedList<>();

      //delete main record
      results.add(KeyValue.pair(key, null));
      getStore().delete(key);

      //delete all annotation references
      if (annStoreValue != null) {
        for (JournalEntryKey annRefKey : annRef.getEntryReferencesList()) {
          results.add(KeyValue.pair(annRefKey, null));
          getStore().delete(key);
        }
      }

      //delete the annotation reference itself
      results.add(KeyValue.pair(annKey, null));
      getStore().delete(annKey);

    } else if (value.getAppendedRecord().getRecordType() == JournalRecordType.ANNOTATE_RECORD) {
      //aggregate the annotation references

      results = new LinkedList<>();

      //new annotation, update our annotation reference list
      AnnotationReference currentAnnotations = AnnotationReference.newBuilder()
          .setMessageId(annKey.getMessageId())
          .addAllEntryReferences(annRef.getEntryReferencesList())
          .addEntryReferences(key)
          .build();

      JournalEntry annEntry = JournalEntry.newBuilder()
          .setAnnotationReference(currentAnnotations)
          .build();

      getStore().put(annKey, ValueAndTimestamp.make(annEntry, timestamp));
      results.add(KeyValue.pair(annKey, annEntry));
    }

    if (SLOG.logger().isDebugEnabled()) {
      results.forEach(kv ->
          SLOG.debug(b -> {
            b.name(getJournalName())
                .putTokens("timestamp", timestamp);

            if (kv.value != null) {
              b.event("ENTRY")
                  .addJournalEntryKey(kv.key)
                  .addJournalEntry(kv.value);
            } else {
              b.event("TOMBSTONE")
                  .addJournalEntryKey(kv.key);
            }
          }));
    }

    return results;
  }
}
