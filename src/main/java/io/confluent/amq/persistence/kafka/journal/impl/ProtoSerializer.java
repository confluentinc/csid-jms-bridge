/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

public class ProtoSerializer<T extends Message> implements Serializer<T> {

  @Override
  public byte[] serialize(String topic, T data) {
    return data == null ? null : data.toByteArray();
  }
}
