/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ReconciledMessage<V> {

  public static ReconciledMessage<Void> tombstone(String topic, JournalRecordKey key) {
    return new ReconciledMessage<>(topic, key, null, TOMBSTONE);
  }

  public static ReconciledMessage<byte[]> route(
      String topic, JournalRecordKey key, JournalRecord value) {

    return new ReconciledMessage<>(topic, key, value, ROUTED);
  }

  public static ReconciledMessage<JournalRecord> forward(
      String topic, JournalRecordKey key, JournalRecord value) {

    return new ReconciledMessage<>(topic, key, value, FORWARD);
  }

  private static final int ROUTED = 0;
  private static final int FORWARD = 1;
  private static final int TOMBSTONE = 2;

  private final String topic;
  private final JournalRecordKey key;
  private final JournalRecord value;
  private final int disposition;


  public ReconciledMessage(
      String topic, JournalRecordKey key, JournalRecord value, int disposition) {

    this.topic = topic;
    this.key = key;
    this.value = value;
    this.disposition = disposition;
  }

  public String getTopic() {
    return topic;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public byte[] getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public byte[] getByteValue() {
    if (value == null) {
      return null;
    }

    if (isForwarded()) {
      JournalRecord jr = (JournalRecord) value;
      return jr.toByteArray();
    } else {
      return (byte[]) value;
    }
  }

  @SuppressWarnings("unchecked")
  public ReconciledMessage<JournalRecord> asForward() {
    if (isForwarded()) {
      return (ReconciledMessage<JournalRecord>) this;
    }

    return null;
  }

  public boolean isTombstoned() {
    return disposition == TOMBSTONE;
  }

  public boolean isRouted() {
    return disposition == ROUTED;
  }

  public boolean isForwarded() {
    return disposition == FORWARD;
  }

  public static class ReconciledMessageSerde implements Serde<ReconciledMessage<?>> {

    @Override
    public Serializer<ReconciledMessage<?>> serializer() {
      return (topic, data) -> {
        if (data == null) {
          return null;
        }
        return data.getByteValue();
      };
    }

    @Override
    public Deserializer<ReconciledMessage<?>> deserializer() {
      return (topic, data) -> {
        throw new UnsupportedOperationException(
            "Deserialization of ReconciledMessage is not supported!");
      };
    }
  }
}
