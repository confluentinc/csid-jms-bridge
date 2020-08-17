/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import java.util.stream.Stream;

public enum ProtocolRecordType {
  UNASSIGNED(0),
  GROUP_RECORD(20),

  // BindingsImpl journal record type
  QUEUE_BINDING_RECORD(21),
  QUEUE_STATUS_RECORD(22),
  ID_COUNTER_RECORD(24),
  ADDRESS_SETTING_RECORD(25),
  SECURITY_RECORD(26),

  // Message journal record types
  ADD_LARGE_MESSAGE_PENDING(29),
  ADD_LARGE_MESSAGE(30),
  ADD_MESSAGE(31),
  ADD_REF(32),
  ACKNOWLEDGE_REF(33),
  UPDATE_DELIVERY_COUNT(34),
  PAGE_TRANSACTION(35),
  SET_SCHEDULED_DELIVERY_TIME(36),
  DUPLICATE_ID(37),
  HEURISTIC_COMPLETION(38),
  ACKNOWLEDGE_CURSOR(39),
  PAGE_CURSOR_COUNTER_VALUE(40),
  PAGE_CURSOR_COUNTER_INC(41),
  PAGE_CURSOR_COMPLETE(42),
  PAGE_CURSOR_PENDING_COUNTER(43),
  ADDRESS_BINDING_RECORD(44),
  ADD_MESSAGE_PROTOCOL(45),
  ADDRESS_STATUS_RECORD(46);

  private final int value;

  ProtocolRecordType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static ProtocolRecordType fromValue(int value) {
    return Stream.of(ProtocolRecordType.values())
        .filter(p -> p.value == value)
        .findFirst()
        .orElse(UNASSIGNED);
  }
}
