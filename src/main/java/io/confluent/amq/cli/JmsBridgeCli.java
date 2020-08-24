/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.model.GlobalMetadata;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.parser.errors.handlers.CollectAll;
import io.confluent.amq.logging.StructuredLogger;

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
    try {
      // Parse with a result to allow us to inspect the results of parsing
      ParseResult<BaseCommand> result = cli.parseWithResult(args);

      if (result.wasSuccessful()) {
        // Parsed successfully, so just run the command and exit
        SLOG.debug(b -> b
            .event("ParseCommand")
            .markSuccess()
            .putTokens("command", result.getCommand().getClass().getSimpleName()));

        //help was requested, either by default command or using -h, --help
        if (result.getCommand().helpRequested()) {
          SLOG.debug(b -> b
              .event("HelpRequested")
              .putTokens("command", result.getCommand().getClass().getSimpleName()));

          result.getCommand().showHelp(io);
          return 0;
        }

        return result.getCommand().execute(io);

      } else {
        SLOG.debug(b -> b
            .event("ParseCommand")
            .markFailure()
            .putTokens("command", result.getCommand().getClass().getSimpleName()));

        return showErrors(cli.getMetadata(), result, args);

      }

    } catch (Exception e) {
      SLOG.debug(
          b -> b.event("ExceptionThrown"),
          e);

      // Errors should be being collected so if anything is thrown it is unexpected
      io.error().println(String.format("Error: %s", e.getMessage()));
      e.printStackTrace(io.error());
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
        .withCommand(ReceiveCommand.class)
        .withCommand(SendCommand.class);

    cliBuilder
        .withGroup("journal")
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

