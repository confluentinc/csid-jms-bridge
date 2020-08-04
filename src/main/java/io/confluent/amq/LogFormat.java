/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

public class LogFormat {
  public static LogFormat forSubject(String subject) {
    return new LogFormat(subject);
  }

  private final String subject;

  public LogFormat(String subject) {
    this.subject = subject;
  }

  public String prefix(String event) {
    return String.format("%s[%s]: ", subject, event);
  }
}
