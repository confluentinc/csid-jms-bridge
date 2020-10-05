/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import java.util.stream.Stream;
import org.apache.activemq.artemis.api.core.Message;

public enum KExMessageType {
  /**
   * This is an empty message, no payload. Contains headers and properties only.
   */
  MESSAGE(Message.DEFAULT_TYPE),
  BYTES(Message.BYTES_TYPE),
  TEXT(Message.TEXT_TYPE),
  OBJECT(Message.OBJECT_TYPE),
  MAP(Message.MAP_TYPE),
  STREAM(Message.STREAM_TYPE),
  UNKNOWN(-1);

  private final int id;

  public static KExMessageType fromId(int id) {
    return Stream.of(KExMessageType.values())
        .filter(t -> t.id == id)
        .findFirst().orElse(UNKNOWN);
  }

  KExMessageType(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
