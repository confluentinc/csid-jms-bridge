/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Arrays;

public class JournalEntryKeyDeserializer implements Deserializer<JournalEntryKey> {

  private final LongDeserializer longDeserializer = new LongDeserializer();
  private final IntegerDeserializer integerDeserializer = new IntegerDeserializer();

  @Override
  public JournalEntryKey deserialize(String topic, byte[] data) {
      if (data == null || data.length == 0) {
          return null;
      }
      if (data.length != 20) {
          throw new SerializationException("Size of data received by JournalEntryKeyDeserializer is not 20 (Long=8, Long=8, Int=4)");
      }
      byte[] messageIdBytes = Arrays.copyOfRange(data, 0, Long.BYTES);
      byte[] txIdBytes = Arrays.copyOfRange(data, Long.BYTES, 2 * Long.BYTES);
      byte[] extendedIdBytes = Arrays.copyOfRange(data, 2 * Long.BYTES, 2 * Long.BYTES + Integer.BYTES);
      return JournalEntryKey.builder()
              .messageId( longDeserializer.deserialize(topic, messageIdBytes))
              .txId(longDeserializer.deserialize(topic, txIdBytes))
              .extendedId(integerDeserializer.deserialize(topic, extendedIdBytes))
              .build();
  }
}
