/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface JmsCnxnSpec {

  String url();

  Optional<String> name();

  Optional<String> clientId();

  class Builder extends JmsCnxnSpec_Builder {

    public Builder() {
      this.url("tcp://localhost:61616");
    }
  }

}
