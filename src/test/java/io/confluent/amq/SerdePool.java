/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class SerdePool {

  private SerdePool() {
  }

  private static final Serde<String> STRING_SERDE = Serdes.String();

  public static byte[] ser(String topic, String value) {
    return STRING_SERDE.serializer().serialize(topic, value);
  }

  public static byte[] ser(String value) {
    return ser("", value);
  }

  public static String deserString(String topic, byte[] value) {
    return STRING_SERDE.deserializer().deserialize(topic, value);
  }

  public static String deserString(byte[] value) {
    return deserString("", value);
  }

}
