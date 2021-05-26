/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.serde;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.amq.logging.StructuredLogger;

public class ProtoDeserializer<T extends Message> implements Deserializer<T> {
  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(ProtoDeserializer.class));

  private final DangerousFunction<byte[], T> protoFn;

  public ProtoDeserializer(
      DangerousFunction<byte[], T> protoFn) {
    this.protoFn = protoFn;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }

    try {
      return protoFn.apply(data);
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("Deserialize")
          .markFailure()
          .message("Failed to deserialize message")
          .putTokens("data", String.format("%02x", data)));
      throw new RuntimeException(e);
    }
  }

  interface DangerousFunction<I, O> {
    O apply(I in) throws Exception;
  }
}
