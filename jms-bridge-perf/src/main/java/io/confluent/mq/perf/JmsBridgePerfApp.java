/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;


public class JmsBridgePerfApp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsBridgePerfApp.class);

  private HTTPServer prometheusServer;

  private int executionStrategy(ParseResult parseResult) {
    int result;
    try {
      init(parseResult.matchedOptionValue("pr-port", 8888));

      int testDelay = parseResult.matchedOptionValue("delay", 0);
      if (testDelay > 0) {
        LOGGER.info("Delaying start by {} seconds", testDelay);
        Thread.sleep(testDelay * 1000);
      }

      result = new CommandLine.RunLast().execute(parseResult); // default execution strategy
      cleanup(parseResult.matchedOptionValue("pr-wait", 0));
    } catch (Exception e) {
      LOGGER.error("Exception occurred during execution.", e);
      result = 1;
    }
    return result;
  }

  private void init(int port) {
    //start prometheus server
    try {
      DefaultExports.initialize();
      prometheusServer = new HTTPServer(port, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void cleanup(int delay) {
    if (null != prometheusServer) {
      try {
        if (delay > 0) {
          LOGGER.info("Delaying shutdown of prometheus server by {} seconds.", delay);
          Thread.sleep(delay * 1000);
        }

        LOGGER.info("Stopping prometheus server");
        prometheusServer.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    JmsBridgePerfApp app = new JmsBridgePerfApp();
    CommandLine cmd = new CommandLine(new JmsBridgeParentCommand());
    cmd.setExecutionStrategy(app::executionStrategy);
    int returnCode = cmd.execute(args);

    if (0 == args.length) {
      cmd.usage(System.out);
    }

    System.exit(returnCode);
  }


}
