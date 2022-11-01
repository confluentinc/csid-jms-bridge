/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.scenarios;

public enum ActivityOptions {
  pub, sub, pubsub;

  public boolean pubActive() {
    return this == pub || this == pubsub;
  }

  public boolean subActive() {
    return this == sub || this == pubsub;
  }
}
