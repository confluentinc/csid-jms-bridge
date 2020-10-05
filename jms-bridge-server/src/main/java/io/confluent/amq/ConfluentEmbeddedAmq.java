/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import org.apache.activemq.artemis.core.server.ActiveMQServer;

public interface ConfluentEmbeddedAmq {
  void start() throws Exception;

  void stop() throws Exception;

  default ActiveMQServer getAmq() {
    return null;
  }

  default ConfluentAmqServer getConfluentAmq() {
    return null;
  }
}
