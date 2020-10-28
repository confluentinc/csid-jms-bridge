/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.model.GlobalMetadata;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.parser.errors.handlers.CollectAll;
import io.confluent.amq.logging.StructuredLogger;
import java.util.Collections;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class JmsBridgeCli {

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(JmsBridgeCli.class));

  private final CommandIo io;

  public JmsBridgeCli(CommandIo io) {
    this.io = io;
  }

  public JmsBridgeCli() {
    this.io = CommandIo.create();
  }

  public static void main(String[] args) throws Exception {

    JmsBridgeCli jmsBridgeCli = new JmsBridgeCli();
    System.exit(jmsBridgeCli.execute(args));

  }

  protected int execute(String[] args) throws Exception {

    Cli<BaseCommand> cli = buildCli();
    for (final String arg : args) {
      if ("--help".equals(arg) || "-h".equals(arg)) {
        Help.help(cli.getMetadata(), Collections.emptyList());
        return 0;
      }
    }

    try {
      ParseResult<BaseCommand> parseResult = cli.parseWithResult(args);
      if (parseResult.wasSuccessful()) {
        return parseResult.getCommand().execute(io);
      } else {
        for (ParseException e : parseResult.getErrors()) {
          io.output().println("Error: " + e.getMessage());
        }
        io.output().println("See the -h or --help flags for usage information");
      }
    } catch (Exception e) {
      io.output().println("Unknown error occurred: " + e.getMessage());
      e.printStackTrace(io.output());
    }
    return 1;
  }

  protected Cli<BaseCommand> buildCli() {
    CliBuilder<BaseCommand> cliBuilder = Cli.<BaseCommand>builder("jms-bridge")
        .withDescription("jms-bridge command line utility")
        .withDefaultCommand(JmsBridgeCliHelp.class)
        .withCommand(JmsBridgeCliHelp.class);

    cliBuilder.withParser()
        .withErrorHandler(new CollectAll());

    cliBuilder
        .withGroup("jms")
        .withDefaultCommand(ReceiveCommand.class)
        .withCommand(ReceiveCommand.class)
        .withCommand(SendCommand.class);

    cliBuilder
        .withGroup("journal")
        .withDefaultCommand(ReadJournalCommand.class)
        .withCommand(ReadJournalCommand.class);

    return cliBuilder.build();
  }

  protected int showErrors(
      GlobalMetadata<?> metadata, ParseResult<BaseCommand> result, String[] args) throws Exception {

    if (result.getCommand() != null && result.getCommand().helpRequested()) {
      result.getCommand().showHelp(io);
      return 0;

    } else {
      // Display the errors
      result.getErrors().stream()
          .map(ParseException::getMessage)
          .forEach(io.output()::println);
      return 1;
    }
  }
}

