/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "jms-bridge-server-start", description = "Start the JMS Bridge server.")
public class JmsBridgeMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsBridgeMain.class);

  @Option(name = "--vanilla",
      hidden = true,
      description = "Startup a vanilla embedded AMQ server instead of the JMS-Bridge")
  protected boolean useVanilla = false;

  @Option(name = "--broker-xml", hidden = true)
  protected String brokerXml;

  @Arguments(description = "The path to a file containing configuration details.")
  protected String configPath;

  public static void main(final String[] args) {
    final JmsBridgeMain jmsBridgeMain = new JmsBridgeMain();
    final int status = jmsBridgeMain.execute(args, JmsBridgeMain.class);
    if (status != 0) {
      System.exit(status);
    }
  }

  protected <T extends JmsBridgeMain> int execute(final String[] args, final Class<T> cmdClazz) {

    final SingleCommand<T> parser = SingleCommand.singleCommand(cmdClazz);
    final JmsBridgeMain cmd = parser.parse(args);
    try {
      cmd.run();
    } catch (Throwable t) {
      LOGGER.error("Failed to start JMS-Bridge.", t);
      return 1;
    }
    return 0;
  }

  protected String brokerXmlPath(final File propFile, final String brokerXmlOpt) {
    if (brokerXmlOpt == null) {
      try {
        final Path brokerXmlPath = propFile.toPath().resolveSibling("broker.xml");
        return brokerXmlPath.toUri().toString();
      } catch (InvalidPathException e) {
        //not found there
      }
    }
    return brokerXmlOpt;
  }

  protected ConfluentEmbeddedAmq loadServer(final BridgeConfig bridgeConfig,
      final String brokerXmlPath) {

    if (!useVanilla) {
      return new ConfluentEmbeddedAmqImpl
          .Builder(brokerXmlPath, bridgeConfig).build();
    } else {
      LOGGER.info("Starting vanilla embedded AMQ server. No JMS-Bridge functionality included.");
      return defaultEmbeddedServer(brokerXmlPath);
    }
  }

  private ConfluentEmbeddedAmq defaultEmbeddedServer(final String brokerXmlPath) {
    final EmbeddedActiveMQ embeddedAmqServer = new EmbeddedActiveMQ();
    embeddedAmqServer.setConfigResourcePath(brokerXmlPath);
    return new ConfluentEmbeddedAmq() {
      @Override
      public void start() throws Exception {
        embeddedAmqServer.start();
      }

      @Override
      public void stop() throws Exception {
        embeddedAmqServer.stop();
      }
    };

  }

  public void run() {

    final File configFile = new File(configPath);
    if (!configFile.exists()) {
      throw new RuntimeException(
          "Configuration file path does not exist: " + configPath);
    } else {
      LOGGER.debug("Loading configuration file: " + configPath);
    }

    final BridgeConfig bridgeConfig = BridgeConfigFactory
        .gatherConfiguration(configFile.toPath());

    final String brokerXmlPath = brokerXmlPath(configFile, brokerXml);
    final ConfluentEmbeddedAmq embeddedActiveMQ = loadServer(bridgeConfig, brokerXmlPath);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        embeddedActiveMQ.stop();
      } catch (Exception e) {
        LOGGER.error("Exception occurred during JMS Bridge server stop.", e);
        throw new RuntimeException(e);
      }
    }));

    try {
      embeddedActiveMQ.start();
    } catch (Throwable t) {
      LOGGER.error("Exception occurred during JMS Bridge server startup.", t);
      throw new RuntimeException(t);
    }
  }

}
