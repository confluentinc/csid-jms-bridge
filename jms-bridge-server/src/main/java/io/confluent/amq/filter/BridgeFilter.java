/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.filter;

public interface BridgeFilter {
  BridgeFilter FILTER_NONE = new BridgeFilter() {
    @Override
    public boolean match(FilterSupport supported) {
      return true;
    }

    @Override
    public String getFilterString() {
      return "FILTER_NONE";
    }
  };

  boolean match(FilterSupport supported);

  String getFilterString();

}
