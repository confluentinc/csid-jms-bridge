/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.Channels;
import java.io.InputStream;
import java.io.PrintStream;

public interface CommandIo {

  static CommandIo create() {
    return new CommandIo() {
      @Override
      public PrintStream error() {
        return Channels.error();
      }

      @Override
      public PrintStream output() {
        return Channels.output();
      }

      @Override
      public InputStream input() {
        return Channels.input();
      }
    };
  }

  PrintStream error();

  PrintStream output();

  InputStream input();
}
