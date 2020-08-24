/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.logging;

import io.confluent.amq.logging.LogSpec.Builder;
import java.util.function.Consumer;

public class LogFormat {

  public static LogFormat forSubject(String subject) {
    return new LogFormat(subject);
  }

  public String build(Consumer<Builder> specWriter) {
    Builder specBuilder = new Builder();
    specWriter.accept(specBuilder);
    LogSpec spec = specBuilder.build();
    return build(spec);
  }

  public String build(LogSpec.Builder specBuilder) {
    return build(specBuilder.build());
  }

  public String build(LogSpec logSpec) {
    return String.format("%s[%s%s]: %s",
        subject,
        logSpec.event(),
        logSpec.eventResult().map(s -> ">" + s).orElse(""),
        logSpec.getKeyValString());
  }

  private final String subject;

  public LogFormat(String subject) {
    this.subject = subject;
  }

  public String prefix(String event) {
    return String.format("%s[%s]: ", subject, event);
  }

}
