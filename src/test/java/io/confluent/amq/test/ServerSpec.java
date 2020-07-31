/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import java.util.Map;
import java.util.Properties;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface ServerSpec {

  default Properties jmsBridgeProps() {
    Properties props = new Properties();
    jmsBridgeConfigs().forEach(props::put);
    return props;
  }

  Map<String, Object> jmsBridgeConfigs();

  String groupId();

  String bridgeId();

  String dataDirectory();

  String brokerXml();

  class Builder extends ServerSpec_Builder {

    public Builder jmsBridgeProps(Properties jmsBridgeProps) {
      jmsBridgeProps.forEach((k, v) -> this.putJmsBridgeConfigs((String)k, v));
      return this;
    }

    @Override
    public ServerSpec build() {
      this.putJmsBridgeConfigs("bridge.id", bridgeId())
        .putJmsBridgeConfigs("group.id", groupId());
      return super.build();
    }
  }
}
