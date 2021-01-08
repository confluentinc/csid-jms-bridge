/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.logging.LogSpec;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournal;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Reads through a kafka journal and describes all of the records in it.
 */
public class KafkaJournalDescriber {

  private final LogFormat logger;
  private final Map<String, Object> kafkaProps;
  private final String journalName;
  private final String bridgeId;

  public KafkaJournalDescriber(Map<String, Object> kafkaProps, String journalName,
      String bridgeId) {

    this.kafkaProps = kafkaProps;
    this.journalName = journalName;
    this.bridgeId = bridgeId;
    this.logger = LogFormat.forSubject(bridgeId);
  }

  public long read(PrintStream out) {
    long count = 0;

    ByteArrayDeserializer deser = new ByteArrayDeserializer();
    try (KafkaConsumer<byte[], byte[]> kafkaConsumer =
        new KafkaConsumer<>(this.kafkaProps, deser, deser)) {

      String topic = KafkaJournal.journalTableTopic(this.bridgeId, this.journalName);

      List<TopicPartition> assignment = kafkaConsumer.partitionsFor(topic, Duration.ofSeconds(5))
          .stream()
          .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
          .collect(Collectors.toList());

      kafkaConsumer.assign(assignment);
      kafkaConsumer.seekToBeginning(assignment);
      while (true) {

        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(5000));
        if (records.isEmpty()) {
          break;
        }

        records.forEach(r -> writeRecord(out, r));
        count = count + records.count();
      }
    }

    return count;
  }

  private void writeRecord(PrintStream out, ConsumerRecord<byte[], byte[]> record) {
    JournalEntryKey key;
    JournalRecord value;
    LogSpec.Builder logSpec = new LogSpec.Builder()
        .name(journalName)
        .addRecordMetadata(record);

    try {
      key = JournalEntryKey.parseFrom(record.key());
    } catch (Exception e) {
      key = null;
    }
    logSpec.addJournalEntryKey(key);

    if (record.value() == null) {
      out.println(logger.build(b -> b
          .mergeFrom(logSpec)
          .event("Tombstone")));
    } else {

      try {
        value = JournalRecord.parseFrom(record.value());
      } catch (Exception e) {
        value = null;
      }
      logSpec.addJournalRecord(value);

      out.println(logger.build(b -> b
          .mergeFrom(logSpec)
          .event("Record")));
    }
  }
}
