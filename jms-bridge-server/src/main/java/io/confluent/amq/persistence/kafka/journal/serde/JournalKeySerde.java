/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JournalKeySerde implements Serde<JournalEntryKey> {
  public static final JournalKeySerde DEFAULT = new JournalKeySerde();

  private final Serializer<JournalEntryKey> serializer;
  private final Deserializer<JournalEntryKey> deserializer;

  public JournalKeySerde() {
    this.serializer = new JournalEntryKeySerializer();
    this.deserializer = new JournalEntryKeyDeserializer();
  }

  @Override
  public Serializer<JournalEntryKey> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<JournalEntryKey> deserializer() {
    return deserializer;
  }
}
