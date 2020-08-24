/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.serde;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JournalValueSerde implements Serde<JournalEntry> {
  public static final JournalValueSerde DEFAULT = new JournalValueSerde();

  private final Serializer<JournalEntry> serializer;
  private final Deserializer<JournalEntry> deserializer;

  public JournalValueSerde() {

    this.serializer = new ProtoSerializer<>();
    this.deserializer = new ProtoDeserializer<>(JournalEntry::parseFrom);
  }

  @Override
  public Serializer<JournalEntry> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<JournalEntry> deserializer() {
    return deserializer;
  }
}
