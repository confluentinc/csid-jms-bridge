/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.serde;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;

public class JournalEntryKeySerializer implements Serializer<JournalEntryKey> {
  private LongSerializer longSerializer = new LongSerializer();
  private IntegerSerializer integerSerializer = new IntegerSerializer();
  @Override
  public byte[] serialize(String topic, JournalEntryKey data) {
    if (data == null) {
      return null;
    }
    byte[] messageId = longSerializer.serialize(topic, data.getMessageId());
    byte[] txId = longSerializer.serialize(topic, data.getTxId());
    byte[] extendedId = integerSerializer.serialize(topic, data.getExtendedId());
    byte[] encoded = new byte[2 * Long.BYTES + Integer.BYTES];
    System.arraycopy(messageId, 0, encoded, 0, Long.BYTES);
    System.arraycopy(txId, 0, encoded, Long.BYTES, Long.BYTES);
    System.arraycopy(extendedId, 0, encoded, 2 * Long.BYTES, Integer.BYTES);
    return encoded;
  }
}
