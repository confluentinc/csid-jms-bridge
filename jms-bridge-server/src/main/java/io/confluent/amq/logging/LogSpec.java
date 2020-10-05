/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.logging;

import io.confluent.amq.persistence.domain.proto.AnnotationReference;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface LogSpec {

  default String getKeyValString() {
    StringBuilder sb = new StringBuilder();

    name().ifPresent(s -> sb.append("name='").append(s).append("', "));
    message().ifPresent(s -> sb.append("msg='").append(s).append("', "));

    tokens().forEach((k, v) -> sb.append(k).append("='").append(v).append("', "));

    if (sb.length() > 2) {
      sb.delete(sb.length() - 2, sb.length());
    }

    return sb.toString();
  }

  String event();

  Optional<String> eventResult();

  Map<String, Object> tokens();

  Optional<String> name();

  Optional<String> message();

  Optional<Throwable> error();

  class Builder extends LogSpec_Builder {

    public Builder addJournalEntryKey(JournalEntryKey record) {
      if (record == null) {
        return this.putTokens("journalEntryKey", "null");
      }

      return this
          .putTokens("txId", record.getTxId())
          .putTokens("messageId", record.getMessageId())
          .putTokens("extendedId", record.getExtendedId());
    }

    public Builder markFailure() {
      return this.eventResult("Failure");
    }

    public Builder markSuccess() {
      return this.eventResult("Success");
    }

    public Builder markStarted() {
      return this.eventResult("Started");
    }

    public Builder markCompleted() {
      return this.eventResult("Completed");
    }

    public Builder addJournalEntry(JournalEntry record) {
      if (record == null) {
        return this.putTokens("journalEntry", "null");
      }

      if (record.hasAppendedRecord()) {
        return addJournalRecord(record.getAppendedRecord());
      } else if (record.hasAnnotationReference()) {
        return addAnnotations(record.getAnnotationReference());
      } else if (record.hasTransactionReference()) {
        return this
            .putTokens("entryType", "TransactionReference")
            .putTokens("referenceCount",
                record.getTransactionReference().getEntryReferencesCount());
      } else {
        return this.putTokens("journalEntry", "invalid");
      }
    }

    public Builder addAnnotations(AnnotationReference annotations) {
      if (annotations == null) {
        return this.putTokens("annotationReference", "null");
      }
      return this.putTokens("entryType", "AnnotationReference")
          .putTokens("messageId", annotations.getMessageId())
          .putTokens("annotationCount", annotations.getEntryReferencesCount());
    }

    public Builder addJournalRecord(JournalRecord record) {
      if (record == null) {
        return this.putTokens("journalRecord", "null");
      }

      return this
          .putTokens("entryType", "JournalRecord")
          .putTokens("messageId", record.getMessageId())
          .putTokens("txId", record.getTxId())
          .putTokens("recordType", record.getRecordType().name())
          .putTokens("protocolRecordType",
              ProtocolRecordType.fromValue(record.getProtocolRecordType()).name())
          .putTokens("size", record.getSerializedSize());
    }

    public Builder addProducerRecord(ProducerRecord<?, ?> rec) {
      if (rec == null) {
        return this.putTokens("producerRecord", "null");
      }

      return this
          .putTokens("producerRecord", rec);
    }

    public Builder addRecordMetadata(ConsumerRecord<?, ?> consumerRecord) {
      if (consumerRecord == null) {
        return this.putTokens("recordMetadata", "null");
      }

      return this
          .putTokens("topic", consumerRecord.topic())
          .putTokens("partition", consumerRecord.partition())
          .putTokens("offset", consumerRecord.offset());
    }

    public Builder addRecordMetadata(RecordMetadata meta) {
      if (meta == null) {
        return this.putTokens("recordMetadata", "null");
      }

      return this
          .putTokens("topic", meta.topic())
          .putTokens("partition", meta.partition())
          .putTokens("offset", meta.offset());
    }

    public Builder addProcessorContext(ProcessorContext context) {
      if (context == null) {
        return this.putTokens("processorContext", "null");
      }

      return this
          .putTokens("processorContext", String.format("[topic=%s, partition=%d, offset=%d]",
              context.topic(), context.partition(), context.offset()));
    }

  }
}

