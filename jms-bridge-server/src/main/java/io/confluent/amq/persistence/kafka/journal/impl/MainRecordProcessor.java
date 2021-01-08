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

public class MainRecordProcessor extends JournalStreamTransformer<JournalEntry> {

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
      SLOG.trace(b -> b
          .addJournalEntryKey(key)
          .addJournalEntry(value)
          .event("RecordPassThrough"));
      return Collections.singletonList(KeyValue.pair(key, value));
    }

    List<KeyValue<JournalEntryKey, JournalEntry>> results = Collections.emptyList();
    JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);
    JournalEntry annStoreValue = getStore().get(annKey);

    long timestamp = getContext().timestamp();
    AnnotationReference annRef;
    if (annStoreValue != null) {
      annRef = annStoreValue.getAnnotationReference();
    } else {
      annRef = AnnotationReference.getDefaultInstance();
    }

    if (value.getAppendedRecord().getRecordType() == JournalRecordType.DELETE_RECORD) {
      results = handleDelete(annRef, key);

    } else if (value.getAppendedRecord().getRecordType() == JournalRecordType.ANNOTATE_RECORD) {
      results = handleAnnotation(annRef, key, value, timestamp);

    }

    logResults(SLOG, results);
    return results;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> handleDelete(
      AnnotationReference currentRefs, JournalEntryKey key) {

    final List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    final JournalEntryKey annRefKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //delete the annotation reference itself
    results.add(KeyValue.pair(annRefKey, null));

    //delete all annotation references
    if (currentRefs != AnnotationReference.getDefaultInstance()) {
      for (JournalEntryKey annKey : currentRefs.getEntryReferencesList()) {
        results.add(KeyValue.pair(annKey, null));
      }
    }

    //delete main record
    results.add(KeyValue.pair(key, null));

    return results;
  }

  //aggregate annotation references for the message Id
  private List<KeyValue<JournalEntryKey, JournalEntry>> handleAnnotation(
      AnnotationReference currentRefs, JournalEntryKey key, JournalEntry value, long timestamp) {

    final List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    final JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //add the annotation first
    results.add(KeyValue.pair(key, value));

    //new annotation, update our annotation reference list
    AnnotationReference updatedAnnotations = AnnotationReference.newBuilder()
        .setMessageId(annKey.getMessageId())
        .addAllEntryReferences(currentRefs.getEntryReferencesList())
        .addEntryReferences(key)
        .build();

    JournalEntry annEntry = JournalEntry.newBuilder()
        .setAnnotationReference(updatedAnnotations)
        .build();

    //add the updated references
    results.add(KeyValue.pair(annKey, annEntry));

    return results;
  }


}
