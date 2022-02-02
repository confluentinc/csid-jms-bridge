/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

public enum HeaderType {
  //JMS types
  STRING("string"),
  INT("int"),
  LONG("long"),
  BYTES("bytes"),
  BOOLEAN("boolean"),
  CHAR("char"),
  FLOAT("float"),
  DOUBLE("double"),
  SHORT("short"),
  OBJECT("object"),

  //Connector types
  INT64("int64", LONG),
  INT32("int32", INT),
  TEXT("text", STRING),
  DESTINATION("destination", STRING),

  //catch-all
  UNKNOWN("unknown");

  public static HeaderType fromCode(String code) {
    try {
      return valueOf(code.toUpperCase());
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  private final String code;
  private final HeaderType jmsType;

  HeaderType(String code) {
    this.code = code;
    this.jmsType = this;
  }

  HeaderType(String code, HeaderType jmsType) {
    this.code = code;
    this.jmsType = jmsType;
  }

  public String getCode() {
    return code;
  }

  public HeaderType getJmsType() {
    return jmsType;
  }
}
