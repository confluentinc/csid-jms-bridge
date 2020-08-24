/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.help.cli.CliCommandUsageGenerator;
import com.github.rvesse.airline.model.CommandMetadata;
import com.github.rvesse.airline.model.ParserMetadata;
import java.io.IOException;
import java.io.OutputStream;
import javax.inject.Inject;

public abstract class DefaultCommand implements BaseCommand {

  @Inject
  protected HelpOption<BaseCommand> help;

  @Override
  public final int execute(CommandIo io) throws Exception {
    if (helpRequested()) {
      showHelp(io);
      return 0;
    } else {
      return run(io);
    }
  }

  @Override
  public boolean helpRequested() {
    return help.help;
  }

  @Override
  public void showHelp(CommandIo io) {
    help.showHelp(new CliCommandUsageGenerator() {
      @Override
      public <T> void usage(String programName, String[] groupNames, String commandName,
          CommandMetadata command, ParserMetadata<T> parserConfig, OutputStream out)
          throws IOException {
        super.usage(programName, groupNames, commandName, command, parserConfig, io.output());
      }
    });
  }

  public abstract int run(CommandIo io) throws Exception;
}
