/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.logging;

import java.util.function.BiConsumer;
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

  //TRACE
  default void trace(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isTraceEnabled()) {
      SlogHelper.log(this, logger()::trace, logSpec, t);
    }
  }

  default void trace(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isTraceEnabled()) {
      SlogHelper.log(this, logger()::trace, logSpec);
    }
  }

  default <T> void trace(T item, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isTraceEnabled()) {
      SlogHelper.logItem(this, logger()::trace, item, logSpec);
    }
  }

  default <T> void trace(T item, BiConsumer<T, LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isTraceEnabled()) {
      SlogHelper.logItem(this, logger()::trace, item, logSpec, t);
    }
  }

  default <T> void trace(Iterable<T> items, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isTraceEnabled()) {
      SlogHelper.logIterable(this, logger()::trace, items, logSpec);
    }
  }

  //DEBUG
  default void debug(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isDebugEnabled()) {
      SlogHelper.log(this, logger()::debug, logSpec);
    }
  }

  default void debug(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isDebugEnabled()) {
      SlogHelper.log(this, logger()::debug, logSpec, t);
    }
  }

  default <T> void debug(T item, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isDebugEnabled()) {
      SlogHelper.logItem(this, logger()::debug, item, logSpec);
    }
  }

  default <T> void debug(T item, BiConsumer<T, LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isDebugEnabled()) {
      SlogHelper.logItem(this, logger()::debug, item, logSpec, t);
    }
  }

  default <T> void debugs(Iterable<T> items, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isDebugEnabled()) {
      SlogHelper.logIterable(this, logger()::debug, items, logSpec);
    }
  }

  //INFO
  default void info(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isInfoEnabled()) {
      SlogHelper.log(this, logger()::info, logSpec, t);
    }
  }

  default void info(Consumer<LogSpec.Builder> logSpec) {
    if (logger().isInfoEnabled()) {
      SlogHelper.log(this, logger()::info, logSpec);
    }
  }

  default <T> void info(T item, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isInfoEnabled()) {
      SlogHelper.logItem(this, logger()::info, item, logSpec);
    }
  }

  default <T> void info(T item, BiConsumer<T, LogSpec.Builder> logSpec, Throwable t) {
    if (logger().isInfoEnabled()) {
      SlogHelper.logItem(this, logger()::info, item, logSpec, t);
    }
  }

  default <T> void infos(Iterable<T> items, BiConsumer<T, LogSpec.Builder> logSpec) {
    if (logger().isInfoEnabled()) {
      SlogHelper.logIterable(this, logger()::info, items, logSpec);
    }
  }

  //WARN
  default void warn(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    SlogHelper.log(this, logger()::warn, logSpec, t);
  }

  default void warn(Consumer<LogSpec.Builder> logSpec) {
    SlogHelper.log(this, logger()::warn, logSpec);
  }

  default <T> void warn(T item, BiConsumer<T, LogSpec.Builder> logSpec) {
    SlogHelper.logItem(this, logger()::warn, item, logSpec);
  }

  default <T> void warn(T item, BiConsumer<T, LogSpec.Builder> logSpec, Throwable t) {
    SlogHelper.logItem(this, logger()::warn, item, logSpec, t);
  }

  default <T> void warns(Iterable<T> items, BiConsumer<T, LogSpec.Builder> logSpec) {
    SlogHelper.logIterable(this, logger()::warn, items, logSpec);
  }

  //ERROR
  default void error(Consumer<LogSpec.Builder> logSpec, Throwable t) {
    SlogHelper.log(this, logger()::error, logSpec, t);
  }

  default void error(Consumer<LogSpec.Builder> logSpec) {
    SlogHelper.log(this, logger()::error, logSpec);
  }

  default <T> void error(T item, BiConsumer<T, LogSpec.Builder> logSpec) {
    SlogHelper.logItem(this, logger()::error, item, logSpec);
  }

  default <T> void error(T item, BiConsumer<T, LogSpec.Builder> logSpec, Throwable t) {
    SlogHelper.logItem(this, logger()::error, item, logSpec, t);
  }

  default <T> void errors(Iterable<T> items, BiConsumer<T, LogSpec.Builder> logSpec) {
    SlogHelper.logIterable(this, logger()::error, items, logSpec);
  }

  class SlogHelper {

    private SlogHelper() {
    }

    private static <T> void logItem(
        StructuredLogger slog,
        Consumer<String> logFn,
        T item,
        BiConsumer<T, LogSpec.Builder> specWriter) {

      LogSpec.Builder builder = new LogSpec.Builder();
      specWriter.accept(item, builder);
      logFn.accept(slog.logFormat().build(builder));
    }

    private static <T> void logItem(
        StructuredLogger slog,
        BiConsumer<String, Throwable> logFn,
        T item,
        BiConsumer<T, LogSpec.Builder> specWriter,
        Throwable t) {

      LogSpec.Builder builder = new LogSpec.Builder();
      specWriter.accept(item, builder);
      logFn.accept(slog.logFormat().build(builder), t);
    }

    private static <T> void logIterable(
        StructuredLogger slog,
        Consumer<String> logFn,
        Iterable<T> items,
        BiConsumer<T, LogSpec.Builder> specWriter) {

      for (T item: items) {
        logItem(slog, logFn, item, specWriter);
      }
    }

    private static void log(
        StructuredLogger slog,
        Consumer<String> logFn,
        Consumer<LogSpec.Builder> specWriter) {

      logFn.accept(slog.logFormat().build(specWriter));
    }

    private static void log(
        StructuredLogger slog,
        BiConsumer<String, Throwable> logFn,
        Consumer<LogSpec.Builder> specWriter,
        Throwable t) {

      logFn.accept(slog.logFormat().build(specWriter), t);
    }
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
