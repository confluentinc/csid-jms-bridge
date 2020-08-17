/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;

public class ProtoDeserializer<T extends Message> implements Deserializer<T> {
  private final DangerousFunction<byte[], T> protoFn;

  public ProtoDeserializer(
      DangerousFunction<byte[], T> protoFn) {
    this.protoFn = protoFn;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      return protoFn.apply(data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  interface DangerousFunction<I, O> {
    O apply(I in) throws Exception;
  }
}
