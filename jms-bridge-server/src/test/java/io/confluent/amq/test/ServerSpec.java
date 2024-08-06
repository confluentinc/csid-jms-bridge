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

  Optional<String> dataDirectory();

  String brokerXml();

  boolean useVanilla();

  boolean isBackup();

  class Builder extends ServerSpec_Builder {

    public Builder() {
      this.isBackup(false);
      this.useVanilla(false);
    }

  }
}
