/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.annotations.Group;
import com.github.rvesse.airline.annotations.Parser;
import com.github.rvesse.airline.model.GlobalMetadata;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.parser.errors.handlers.CollectAll;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@com.github.rvesse.airline.annotations.Cli(name = "jms-bridge",
    description = "jms-bridge command line utility",
    groups = {
      @Group(name = "jms",
             defaultCommand = JmsBridgeCliHelp.class,
             commands = {JmsBridgeCliHelp.class, SendCommand.class, ReceiveCommand.class}),
      @Group(name = "journal",
          defaultCommand = JmsBridgeCliHelp.class,
          commands = {JmsBridgeCliHelp.class, ReadJournalCommand.class})
    },
    parserConfiguration = @Parser(
        useDefaultOptionParsers = true,
        defaultParsersFirst = false,
        errorHandler = CollectAll.class
    ))
public class JmsBridgeCli {
  private final CommandIo io;

  public JmsBridgeCli(CommandIo io) {
    this.io = io;
  }

  public JmsBridgeCli() {
    this.io = CommandIo.create();
  }

  public static void main(String[] args) throws Exception {
    JmsBridgeCli cli = new JmsBridgeCli();
    System.exit(cli.execute(args));
  }

  protected int execute(String[] args) throws Exception {
    Cli<BaseCommand> cli = buildCli();

    try {
      // Parse with a result to allow us to inspect the results of parsing
      ParseResult<BaseCommand> result = cli.parseWithResult(args);

      if (result.wasSuccessful()) {
        // Parsed successfully, so just run the command and exit
        return result.getCommand().execute();

      } else {
        return showHelp(cli.getMetadata(), result, args);

      }
    } catch (Exception e) {
      // Errors should be being collected so if anything is thrown it is unexpected
      io.error().println(String.format("Unexpected error: %s", e.getMessage()));
      showHelp(cli.getMetadata(), null, args);
    }

    return 1;
  }

  protected Cli<BaseCommand> buildCli() {
    return new Cli<>(JmsBridgeCli.class);
  }

  protected int showHelp(
      GlobalMetadata<?> metadata, ParseResult<BaseCommand> result, String[] args) throws Exception {

    final boolean helpRequested = helpRequested(args);
    if (result == null) {
      com.github.rvesse.airline.help.Help.help(metadata, Arrays.asList(args),
          io.error());
      return 1;
    } else if (result.getCommand() == null && helpRequested) {
      com.github.rvesse.airline.help.Help.help(metadata, Collections.emptyList(),
          io.output());
      return 0;
    } else if (result.getCommand() == null && !helpRequested) {
      com.github.rvesse.airline.help.Help.help(metadata, Collections.emptyList(),
          io.error());
      return 1;
    } else if (helpRequested) {
      com.github.rvesse.airline.help.Help.help(metadata, Arrays.asList(args),
          io.output());
      return 0;
    } else {
      // Display any errors and then the help information
      int i = 1;
      for (ParseException e : result.getErrors()) {
        io.error().println(String.format("Error %d: %s", i, e.getMessage()));
        i++;
      }
      com.github.rvesse.airline.help.Help.help(metadata, Arrays.asList(args),
          io.error());
      return 1;
    }
  }

  protected static boolean helpRequested(String[] args) {
    return args.length == 0 || Stream.of(args)
        .anyMatch(arg -> "-h".equals(arg) || "--help".equals(arg));
  }
}
