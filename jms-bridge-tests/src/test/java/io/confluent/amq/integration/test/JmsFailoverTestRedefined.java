/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.ConfluentAmqServer;
import java.io.File;
import javax.management.MBeanServer;
import net.bytebuddy.asm.Advice;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.commons.lang3.reflect.FieldUtils;

public class JmsFailoverTestRedefined {


  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static ActiveMQServer enter(@Advice.Argument(0) ActiveMQServer server) {

    if (server != null) {
      Configuration configuration = server.getConfiguration();
      NodeManager nodeManager = server.getNodeManager();
      ActiveMQSecurityManager securityManager = server.getSecurityManager();
      MBeanServer mbeanServer;
      try {
        mbeanServer = (MBeanServer) FieldUtils.readDeclaredField(server, "mbeanServer", true);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      ConfluentAmqServer amqServer;
      if (nodeManager != null) {
        amqServer = new ConfluentAmqServer(
            JmsSuiteRunner.wrapConfig(configuration),
            mbeanServer,
            securityManager) {

          @Override
          protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
            return nodeManager;
          }
        };
      } else {
        amqServer = new ConfluentAmqServer(
            JmsSuiteRunner.wrapConfig(configuration),
            mbeanServer,
            securityManager);
      }
      System.out.println("hear I am");
      return amqServer;

    }

    return server;
  }
}
