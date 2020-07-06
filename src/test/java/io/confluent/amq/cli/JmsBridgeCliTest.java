/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.rvesse.airline.ChannelFactory;
import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.parser.ParseResult;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.jupiter.api.BeforeEach;
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
  public void callNoArgs() throws Exception {
    cli.execute(new String[]{});
    assertUsage(false);
  }

  @Test
  public void callHelpOption() throws Exception {
    cli.execute(new String[]{"--help"});
    assertUsage(false);
  }

  @Test
  public void callHelpCommand() throws Exception {
    cli.execute(new String[]{"help"});
    assertUsage(false);
  }

  @Test
  public void callCommandHelp() throws Exception {
    cli.execute(new String[]{"help", "send"});

    assertCommandHelp("send", false);
  }

  @Test
  public void callCommandHelpOption() throws Exception {
    cli.execute(new String[]{"send", "--help"});

    assertCommandHelp("send", false);
  }

  @Test
  public void callBadArgs() throws Exception {
    cli.execute(new String[]{"bad", "--args"});

    assertUsage(true);
  }

  @Test
  public void callCommandBadArgs() throws Exception {
    cli.execute(new String[]{"send", "--args"});

    List<String> lines = commandIo.readAllErrorLines();
    assertTrue(lines.size() > 0);

    logCli(lines);
    assertEquals("Error 1: Found unexpected parameters: [--args]", lines.get(0));
  }

  @Test
  public void callWithException() throws Exception {
    JmsBridgeCli mycli = new JmsBridgeCli(commandIo) {
      @Override
      protected Cli<BaseCommand> buildCli() {
        return new Cli<BaseCommand>(JmsBridgeCli.class) {
          @Override
          public ParseResult<BaseCommand> parseWithResult(Iterable<String> args) {
            throw new RuntimeException("BOOM");
          }
        };
      }
    };

    mycli.execute(new String[]{"send"});

    List<String> lines = commandIo.readAllErrorLines();
    assertTrue(lines.size() > 0);

    logCli(lines);

    assertEquals("Unexpected error: BOOM", lines.get(0));
  }

  //Helper methods below

  public void assertCommandHelp(String command, boolean fromError) {
    final List<String> lines = readLines(fromError);

    assertTrue(lines.size() > 1);
    logCli(lines);
    assertEquals("NAME", lines.get(0));
    String token = " " + command + " - ";
    assertTrue("Failed to find command name in output: '" + token + "'.",
        lines.get(1).contains(token));

  }

  public void assertUsage(boolean fromError) {
    final List<String> lines = readLines(fromError);

    assertTrue(lines.size() > 0);
    logCli(lines);
    assertEquals("usage: jms-bridge <command> [ <args> ]", lines.get(0));
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
    LOGGER.debug("cli output: {}{}",
        System.lineSeparator(),
        String.join(System.lineSeparator(), cliOutputLines));

  }
}