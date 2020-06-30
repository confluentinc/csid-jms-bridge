/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static java.lang.String.format;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(name = "jms-bridge-server-start", description = "Start the JMS Bridge server.")
public class JmsBridgeMain {

  private static final Logger logger = LoggerFactory.getLogger(JmsBridgeMain.class);

  @Option(name = "--broker-xml", hidden = true)
  protected String brokerXml;

  @Arguments(description = "The path to a properties file containing configuration details.")
  protected String propertiesPath;

  public static void main(final String[] args) {

    SingleCommand<JmsBridgeMain> parser = SingleCommand.singleCommand(JmsBridgeMain.class);
    JmsBridgeMain cmd = parser.parse(args);
    try {
      cmd.run();
    } catch (Throwable t) {
      logger.error("Failed to start JMS-Bridge.", t);
      Runtime.getRuntime().exit(1);
    }
  }

  public void run() {

    final Properties serverProps = new Properties();

    final File propFile = new File(propertiesPath);
    try (InputStream props = Files.newInputStream(propFile.toPath())) {
      serverProps.load(props);
    } catch (IOException e) {
      logger.error(format(
          "Failed to load JMS Bridge properties file from '%s'", propertiesPath), e);
      throw new RuntimeException(e);
    }

    if (brokerXml == null) {

      try {
        Path brokerXmlPath = propFile.toPath().resolveSibling("broker.xml");
        brokerXml = brokerXmlPath.toString();
      } catch (InvalidPathException e) {
        //not found there
      }
    }

    //for now start the default implementation
    final EmbeddedActiveMQ embeddedAmqServer = new EmbeddedActiveMQ();
    embeddedAmqServer.setConfigResourcePath(brokerXml);
    //final ConfluentEmbeddedAmq embeddedAmqServer = new ConfluentEmbeddedAmq.Builder(serverProps)
    //  .build();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        embeddedAmqServer.stop();
      } catch (Exception e) {
        logger.error("Exception occurred during JMS Bridge server stop.", e);
        throw new RuntimeException(e);
      }
    }));

    try {
      embeddedAmqServer.start();
    } catch (Throwable t) {
      logger.error("Exception occurred during JMS Bridge server startup.", t);
      throw new RuntimeException(t);
    }
  }
}
