/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import org.apache.kafka.streams.KeyValue;

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
      JournalEntryKey readOnlyKey, JournalEntry entry) {

    if (entry == null
        || !entry.hasAppendedRecord()
        || !RECORDS_OF_INTEREST.contains(entry.getAppendedRecord().getRecordType())) {
      //pass through
      SLOG.trace(b -> b
          .addJournalEntryKey(readOnlyKey)
          .addJournalEntry(entry)
          .event("RecordPassThrough"));
      return Collections.singletonList(KeyValue.pair(readOnlyKey, entry));
    }

    List<KeyValue<JournalEntryKey, JournalEntry>> results = Collections.emptyList();

    if (entry.getAppendedRecord().getRecordType() == JournalRecordType.DELETE_RECORD) {
      results = handleDelete(readOnlyKey);

    } else if (entry.getAppendedRecord().getRecordType() == JournalRecordType.ANNOTATE_RECORD) {
      results = handleAnnotation(readOnlyKey, entry);

    }

    logResults(SLOG, results);
    return results;
  }

  private List<KeyValue<JournalEntryKey, JournalEntry>> handleDelete(JournalEntryKey key) {

    List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    JournalEntryKey annRefKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //delete the annotation reference itself
    results.add(KeyValue.pair(annRefKey, null));

    //delete main record
    results.add(KeyValue.pair(key, null));

    return results;
  }

  //aggregate annotation references for the message Id
  private List<KeyValue<JournalEntryKey, JournalEntry>> handleAnnotation(
      JournalEntryKey key, JournalEntry value) {

    List<KeyValue<JournalEntryKey, JournalEntry>> results = new LinkedList<>();
    JournalEntryKey annKey = KafkaRecordUtils.annotationsKeyFromRecordKey(key);

    //add the annotation first
    results.add(KeyValue.pair(key, value));

    //new annotation, update our annotation reference list
    AnnotationReference updatedAnnotations = AnnotationReference.newBuilder()
        .setMessageId(annKey.getMessageId())
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
