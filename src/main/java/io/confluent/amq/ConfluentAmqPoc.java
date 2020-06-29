/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import java.nio.file.Files;
import java.util.Properties;
import org.jboss.logging.Logger;

public class ConfluentAmqPoc {

  private static final Logger logger = Logger.getLogger(ConfluentAmqPoc.class);

  public static void main(String... args) throws Exception {

    try {
      final ServerOptions serverOptions = ServerOptions.parse(args);
      if (serverOptions == null) {
        return;
      }

      Properties serverProps = new Properties();

      serverProps.load(Files.newInputStream(serverOptions.getPropertiesFile().toPath()));

      ConfluentEmbeddedAmq embeddedAmqServer = new ConfluentEmbeddedAmq.Builder(serverProps)
          .build();

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          embeddedAmqServer.stop();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));

      embeddedAmqServer.start();

    } catch (final Exception e) {
      logger.error("Failed to start JMS-Bridge Server", e);
      System.exit(-1);
    }
  }
}
