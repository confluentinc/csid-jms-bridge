/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.logging;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

    public Builder addJournalRecordKey(JournalRecordKey record) {
      String key = String.format("tx-%d_id-%d",
          record.getTxId(),
          record.getId());

      return this
          .putTokens("key", key);
    }

    public Builder markFailure() {
      return this.eventResult("Failure");
    }

    public Builder markSuccess() {
      return this.eventResult("Success");
    }

    public Builder addJournalRecord(JournalRecord record) {
      return this
          .putTokens("id", record.getId())
          .putTokens("txId", record.getTxId())
          .putTokens("recordType", record.getRecordType().name())
          .putTokens("userRecordType", record.getUserRecordType())
          .putTokens("recordSize", record.getSerializedSize());
    }

    public Builder addProducerRecord(ProducerRecord<?, ?> rec) {
      return this
          .putTokens("topic", rec.topic())
          .putTokens("partition", rec.partition());
    }

    public Builder addRecordMetadata(RecordMetadata meta) {
      return this
          .putTokens("topic", meta.topic())
          .putTokens("partition", meta.partition())
          .putTokens("offset", meta.offset());
    }

  }
}

