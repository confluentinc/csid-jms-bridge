/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.ConfluentAmqServer;
import java.io.File;
import java.nio.file.Files;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

/**
 * Used for intercepting static method creation of active mq servers in test classes.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class ActiveMQServersRedefined {

  public static ActiveMQServer intercept(
      final Configuration config,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager,
      final boolean enablePersistence) {

    config.setPersistenceEnabled(enablePersistence);
    tryTocreateDirectory(config.getBindingsDirectory());
    tryTocreateDirectory(config.getJournalDirectory());
    tryTocreateDirectory(config.getPagingDirectory());
    tryTocreateDirectory(config.getLargeMessagesDirectory());

    return new ConfluentAmqServer(
        JmsSuiteRunner.wrapConfig(config),
        mbeanServer,
        securityManager);
  }

  public static void tryTocreateDirectory(String path) {
    try {
      Files.createDirectory(new File(path).toPath());
    } catch (Exception e) {
      //
    }

  }
}
