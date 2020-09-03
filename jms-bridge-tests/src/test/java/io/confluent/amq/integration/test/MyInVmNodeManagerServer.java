/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.ConfluentAmqServer;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public class MyInVmNodeManagerServer extends ConfluentAmqServer {

  final NodeManager nodeManager;

  public MyInVmNodeManagerServer(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public MyInVmNodeManagerServer(Configuration configuration, NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration));
    this.nodeManager = nodeManager;
  }

  public MyInVmNodeManagerServer(Configuration configuration, MBeanServer mbeanServer,
      NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), mbeanServer);
    this.nodeManager = nodeManager;
  }

  public MyInVmNodeManagerServer(Configuration configuration,
      ActiveMQSecurityManager securityManager, NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), securityManager);
    this.nodeManager = nodeManager;
  }

  public MyInVmNodeManagerServer(Configuration configuration, MBeanServer mbeanServer,
      ActiveMQSecurityManager securityManager, NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), mbeanServer, securityManager);
    this.nodeManager = nodeManager;
  }

}
