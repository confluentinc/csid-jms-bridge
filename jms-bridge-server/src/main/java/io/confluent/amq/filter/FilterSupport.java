/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.filter;

public interface FilterSupport {
  Object getProperty(String name);
}
