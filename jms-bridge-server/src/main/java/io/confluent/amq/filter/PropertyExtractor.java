/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.filter;

import java.util.Optional;

public interface PropertyExtractor {

  Optional<Object> extract(FilterSupport message);

}
