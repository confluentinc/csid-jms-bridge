/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.help.Help;
import io.confluent.amq.logging.StructuredLogger;
import java.io.IOException;
import javax.inject.Inject;

public class JmsBridgeCliHelp extends Help<Runnable> implements BaseCommand {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(JmsBridgeCliHelp.class));

  @Inject
  HelpOption<Runnable> help;

  public JmsBridgeCliHelp() {
  }

  @Override
  public int execute(CommandIo io) throws Exception {
    showHelp(io);
    return 0;
  }

  @Override
  public boolean helpRequested() {
    return help.help;
  }

  @Override
  public void showHelp(CommandIo io) {
    try {
      help(global, command, this.includeHidden, io.output());
    } catch (IOException e) {
      throw new RuntimeException("Error generating usage documentation", e);
    }
  }
}
