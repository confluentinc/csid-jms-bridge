/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import io.confluent.amq.config.BridgeConfig;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface ServerSpec {

  BridgeConfig jmsBridgeConfig();

  String groupId();

  String bridgeId();

  Optional<String> dataDirectory();

  String brokerXml();

  boolean useVanilla();

  class Builder extends ServerSpec_Builder {

    public Builder() {
      this.useVanilla(false);
    }

  }
}
