/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class TestSupport {

  /**
   * Used for test logging
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(TestSupport.class);

  private TestSupport() {
  }

  public static void println(String format, Object... objects) {
    LOGGER.info(format, objects);
  }
}

