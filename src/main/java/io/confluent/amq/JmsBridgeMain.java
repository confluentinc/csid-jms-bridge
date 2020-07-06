/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(JmsBridgeMain.class);

  @Option(name = "--broker-xml", hidden = true)
  protected String brokerXml;

  @Arguments(description = "The path to a properties file containing configuration details.")
  protected String propertiesPath;

  public static void main(final String[] args) {
    final JmsBridgeMain jmsBridgeMain = new JmsBridgeMain();
    final int status = jmsBridgeMain.execute(args, JmsBridgeMain.class);
    System.exit(status);
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
        return brokerXmlPath.toString();
      } catch (InvalidPathException e) {
        //not found there
      }
    }
    return brokerXmlOpt;
  }

  protected ConfluentEmbeddedAmq loadServer(final Properties serverProps, final String brokerXmlPath) {
    //for now start the default implementation
    final ConfluentEmbeddedAmq confluentEmbeddedAmq = defaultEmbeddedServer(brokerXmlPath);
    //final ConfluentEmbeddedAmq embeddedAmqServer = new ConfluentEmbeddedAmq.Builder(serverProps)
    //  .build();
    return confluentEmbeddedAmq;
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

    final Properties serverProps = new Properties();

    final File propFile = new File(propertiesPath);
    try (InputStream props = Files.newInputStream(propFile.toPath())) {
      serverProps.load(props);
    } catch (IOException e) {
      LOGGER.error("Failed to load JMS Bridge properties file from '{}'", propertiesPath, e);
      throw new RuntimeException(e);
    }

    final String brokerXmlPath = brokerXmlPath(propFile, brokerXml);
    final ConfluentEmbeddedAmq embeddedActiveMQ = loadServer(serverProps, brokerXmlPath);

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
