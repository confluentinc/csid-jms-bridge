/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

public interface ConfluentEmbeddedAmq {
  void start() throws Exception;

  void stop() throws Exception;

}
