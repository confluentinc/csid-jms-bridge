/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.ChannelFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class CapturedCommandIo implements
    CommandIo, ChannelFactory, ParameterResolver, BeforeEachCallback {

  private static final CapturedCommandIo INSTANCE = new CapturedCommandIo();

  private final PipedInputStream errorInPipe;
  private final PipedOutputStream errorOutPipe;
  private final PrintStream errorPrinter;
  private volatile BufferedReader errorReader;

  private final PipedInputStream outputInPipe;
  private final PipedOutputStream outputOutPipe;
  private final PrintStream outputPrinter;
  private volatile BufferedReader outputReader;

  private final PipedOutputStream inputOutPipe;
  private final PipedInputStream inputInPipe;
  private volatile BufferedWriter inputWriter;

  public CapturedCommandIo() {
    try {
      errorInPipe = new PipedInputStream(8192);
      errorOutPipe = new PipedOutputStream(errorInPipe);
      errorPrinter = new PrintStream(errorOutPipe, true, StandardCharsets.UTF_8.name());
      errorReader = new BufferedReader(new InputStreamReader(errorInPipe, StandardCharsets.UTF_8));

      outputInPipe = new PipedInputStream(8192);
      outputOutPipe = new PipedOutputStream(outputInPipe);
      outputPrinter = new PrintStream(outputOutPipe, true, StandardCharsets.UTF_8.name());
      outputReader = new BufferedReader(
          new InputStreamReader(outputInPipe, StandardCharsets.UTF_8));

      inputOutPipe = new PipedOutputStream();
      inputInPipe = new PipedInputStream(inputOutPipe, 8192);
      inputWriter = new BufferedWriter(
          new OutputStreamWriter(inputOutPipe, StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void reset() {
    try {

      while (INSTANCE.errorReader.ready() && INSTANCE.errorReader.read() != -1) {
        //skip
      }
      while (INSTANCE.outputReader.ready() && INSTANCE.outputReader.read() != -1) {
        //skip
      }
      INSTANCE.inputWriter.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> readAllErrorLines() {
    return readAllLines(errorReader());
  }

  public List<String> readAllOutputLines() {
    return readAllLines(outputReader());
  }

  private List<String> readAllLines(BufferedReader reader) {
    List<String> lines = new ArrayList<>();
    String line;
    try {
      while (reader.ready() && (line = reader.readLine()) != null) {
        lines.add(line);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return lines;
  }

  public BufferedReader errorReader() {
    return INSTANCE.errorReader;
  }

  public BufferedReader outputReader() {
    return INSTANCE.outputReader;
  }

  public BufferedWriter inputWriter() {
    return INSTANCE.inputWriter;
  }

  @Override
  public PrintStream createOutput() {
    return INSTANCE.outputPrinter;
  }

  @Override
  public PrintStream createError() {
    return INSTANCE.errorPrinter;
  }

  @Override
  public InputStream createInput() {
    return INSTANCE.inputInPipe;
  }

  @Override
  public PrintStream error() {
    return INSTANCE.errorPrinter;
  }

  @Override
  public PrintStream output() {
    return INSTANCE.outputPrinter;
  }

  @Override
  public InputStream input() {
    return INSTANCE.inputInPipe;
  }

  //JUNIT SUPPORT

  @Override
  public boolean supportsParameter(ParameterContext parameterContext,
      ExtensionContext extensionContext) throws ParameterResolutionException {

    return parameterContext.getParameter().getType().equals(CapturedCommandIo.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext,
      ExtensionContext extensionContext) throws ParameterResolutionException {

    return INSTANCE;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    INSTANCE.reset();
  }
}
