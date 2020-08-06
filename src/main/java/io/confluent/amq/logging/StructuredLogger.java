/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.logging;

import java.util.function.Consumer;
import org.inferred.freebuilder.FreeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FreeBuilder
public interface StructuredLogger {

  static StructuredLogger with(Consumer<Builder> specBuilder) {
    Builder spec = new Builder();
    specBuilder.accept(spec);
    return spec.build();
  }

  Logger logger();

  LogFormat logFormat();

  default void trace(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isTraceEnabled()) {
      logger().trace(logFormat().build(logSpec), t);
    }
  }

  default void trace(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isTraceEnabled()) {
      logger().trace(logFormat().build(logSpec));
    }
  }

  default void debug(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isDebugEnabled()) {
      logger().debug(logFormat().build(logSpec));
    }
  }

  default void debug(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isDebugEnabled()) {
      logger().debug(logFormat().build(logSpec), t);
    }
  }

  default void info(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isInfoEnabled()) {
      logger().info(logFormat().build(logSpec), t);
    }
  }

  default void info(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isInfoEnabled()) {
      logger().info(logFormat().build(logSpec));
    }
  }

  default void warn(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    logger().warn(logFormat().build(logSpec), t);
  }

  default void warn(Consumer<LogSpec.Builder> logSpec) {
    logger().warn(logFormat().build(logSpec));
  }

  default void error(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    logger().error(logFormat().build(logSpec), t);
  }

  default void error(Consumer<LogSpec.Builder> logSpec) {
    logger().error(logFormat().build(logSpec));
  }

  class Builder extends StructuredLogger_Builder {

    private Class<?> loggerClass;

    public Builder loggerClass(Class<?> clazz) {
      this.loggerClass = clazz;
      return this;
    }

    @Override
    public StructuredLogger build() {

      if (loggerClass != null) {

        try {
          this.logger();
        } catch (IllegalStateException e) {
          logger(LoggerFactory.getLogger(loggerClass));
        }

        try {
          this.logFormat();
        } catch (IllegalStateException e) {
          logFormat(LogFormat.forSubject(loggerClass.getSimpleName()));
        }

      }

      return super.build();
    }
  }
}
