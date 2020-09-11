/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.filter;

import io.confluent.amq.filter.impl.ExpressionFactoryImpl;

public interface ExpressionFactory {

  static ExpressionFactory getInstance() {
    return ExpressionFactoryImpl.getInstance();
  }

  BridgeFilter parseAmqFilter(String sql);

  PropertyExtractor parsePropertyExtractor(String sql);
}
