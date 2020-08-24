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
      results = handleDelete(annRef, key);

    } else if (value.getAppendedRecord().getRecordType() == JournalRecordType.ANNOTATE_RECORD) {
      results = handleAnnotation(annRef, key, timestamp);

    }

    logResults(SLOG, results);
    return results;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> handleDelete(
      AnnotationReference currentRefs, JournalEntryKey key) {

    final List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    final JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //delete main record
    results.add(KeyValue.pair(key, null));
    getStore().delete(key);

    //delete all annotation references
    if (currentRefs != AnnotationReference.getDefaultInstance()) {
      for (JournalEntryKey annRefKey : currentRefs.getEntryReferencesList()) {
        results.add(KeyValue.pair(annRefKey, null));
        getStore().delete(key);
      }
    }

    //delete the annotation reference itself
    results.add(KeyValue.pair(annKey, null));
    getStore().delete(annKey);

    return results;
  }

  //aggregate annotation references for the message Id
  private List<KeyValue<JournalEntryKey, JournalEntry>> handleAnnotation(
      AnnotationReference annRef, JournalEntryKey key, long timestamp) {

    final List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    final JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //new annotation, update our annotation reference list
    AnnotationReference updatedAnnotations = AnnotationReference.newBuilder()
        .setMessageId(annKey.getMessageId())
        .addAllEntryReferences(annRef.getEntryReferencesList())
        .addEntryReferences(key)
        .build();

    JournalEntry annEntry = JournalEntry.newBuilder()
        .setAnnotationReference(updatedAnnotations)
        .build();

    getStore().put(annKey, ValueAndTimestamp.make(annEntry, timestamp));
    results.add(KeyValue.pair(annKey, annEntry));

    return results;
  }


}
