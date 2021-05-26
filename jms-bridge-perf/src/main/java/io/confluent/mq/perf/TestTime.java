/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf;

public class TestTime {

  private final long startMillis;

  public TestTime() {
    this.startMillis = System.currentTimeMillis();
  }

  public long startTime() {
    return this.startMillis;
  }
}
