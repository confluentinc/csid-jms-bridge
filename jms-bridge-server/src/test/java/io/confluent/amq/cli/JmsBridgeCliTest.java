/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.rvesse.airline.ChannelFactory;
import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.parser.ParseResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsBridgeCliTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsBridgeCliTest.class);

  static CapturedCommandIo commandIo;

  static {
    commandIo = (CapturedCommandIo) ServiceLoader.load(ChannelFactory.class).iterator().next();
  }

  JmsBridgeCli cli;

  @BeforeEach
  public void setup() {
    commandIo.reset();
    cli = new JmsBridgeCli(commandIo);
  }

  @Test
  @DisplayName("call without any arguments")
  public void callNoArgs() throws Exception {
    cli.execute(new String[]{});
    assertUsage();
  }

  @Test
  @DisplayName("call: --help")
  public void callHelpOption() throws Exception {
    cli.execute(new String[]{"--help"});
    assertUsage();
  }

  @Test
  @DisplayName("call: help")
  public void callHelpCommand() throws Exception {
    cli.execute(new String[]{"help"});
    assertUsage();
  }

  @Test
  @DisplayName("call: help jms send")
  public void callHelpSend() throws Exception {
    cli.execute(new String[]{"help", "jms", "send"});

    assertCommandHelp("jms send");
  }

  @Test
  @DisplayName("call: jms send --help")
  public void callSendWithHelpOption() throws Exception {
    cli.execute(new String[]{"jms", "send", "--help"});

    assertUsage();
  }

  @Test
  @DisplayName("call with invalid command chain: send")
  public void callBadArgs() throws Exception {
    cli.execute(new String[]{"send"});
    assertLineFound("Unknown command send"::equals);
  }

  @Test
  @DisplayName("call with invalid help command chain: help send")
  public void callInvalidHelpCommandChain() throws Exception {
    cli.execute(new String[]{"help", "send"});
    assertLinesFound(Arrays.asList(
        "Unknown command send"::equals, l -> l.contains("-h or --help")));
  }

  @Test
  @DisplayName("call with invalid option: jms send --args")
  public void callCommandBadArgs() throws Exception {
    cli.execute(new String[]{"jms", "send", "--args"});

    assertLineFound("Error: Found unexpected parameters: [--args]"::equals);
  }

  @Test
  @DisplayName("call throws an exception")
  public void callWithException() throws Exception {
    JmsBridgeCli mycli = new JmsBridgeCli(commandIo) {
      @Override
      protected Cli<BaseCommand> buildCli() {
        return new Cli<BaseCommand>(super.buildCli().getMetadata()) {
          @Override
          public ParseResult<BaseCommand> parseWithResult(Iterable<String> args) {
            throw new RuntimeException("BOOM");
          }

          @Override
          public BaseCommand parse(String... args) {
            throw new RuntimeException("BOOM");
          }
        };
      }
    };

    mycli.execute(new String[]{"jms"});
    assertLineFound(l -> l.contains("BOOM"));
  }

  //Helper methods below

  public void assertCommandHelp(String command) {
    assertCommandHelp(command, false);
  }

  public void assertCommandHelp(String command, boolean fromError) {
    List<String> lines = assertLinesFound(Arrays.asList(
        "NAME"::equals, "SYNOPSIS"::equals, "OPTIONS"::equals
    ));

    final String fullCommand = "jms-bridge " + command;
    assertTrue(lines.stream().anyMatch(l -> l.contains(fullCommand)),
        "Found unexpected command output");

  }

  public List<String> assertLineFound(Predicate<String> match) {
    return assertLineFound(match, false);
  }

  public List<String> assertLineFound(Predicate<String> match, boolean fromError) {
    final List<String> lines = readLines(fromError);
    logCli(lines);
    assertTrue(lines.stream().anyMatch(match),
        "Found unexpected command output");

    return lines;
  }

  public List<String> assertLinesFound(Iterable<Predicate<String>> lines) {
    return assertLinesFound(lines, false);
  }

  public List<String> assertLinesFound(Iterable<Predicate<String>> matches, boolean fromError) {
    final List<String> lines = readLines(fromError);
    logCli(lines);

    for (Predicate<String> match : matches) {
      assertTrue(lines.stream().anyMatch(match),
          String.format("Expected %s output to contain line: '%s'",
              fromError ? "error" : "standard",
              match));

    }

    return lines;
  }

  public void assertUsage() {
    assertUsage(false);
  }

  public void assertUsage(boolean fromError) {
    assertLineFound("usage: jms-bridge <command> [ <args> ]"::equals, fromError);
  }

  public List<String> readLines(boolean fromError) {
    final List<String> lines = new ArrayList<>();

    if (fromError) {
      lines.addAll(commandIo.readAllErrorLines());
    } else {
      lines.addAll(commandIo.readAllOutputLines());
    }

    return lines;
  }

  public void logCli(List<String> cliOutputLines) {
    LOGGER.info("cli output: {}{}",
        System.lineSeparator(),
        String.join(System.lineSeparator(), cliOutputLines));

  }
}