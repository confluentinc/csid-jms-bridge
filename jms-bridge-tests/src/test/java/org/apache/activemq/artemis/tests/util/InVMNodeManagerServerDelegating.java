/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.util;

import io.confluent.amq.DelegatingConfluentAmqServer;
import io.confluent.amq.integration.test.JmsSuiteRunner;
import java.io.File;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

public final class InVMNodeManagerServerDelegating extends DelegatingConfluentAmqServer {

  final NodeManager nodeManager;

  public InVMNodeManagerServerDelegating(final NodeManager nodeManager) {
    super();
    System.out.println("Creating InVMNodeManagerServer for ConfluentAmqServer");
    this.nodeManager = nodeManager;
  }

  public InVMNodeManagerServerDelegating(final Configuration configuration, final NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration));
    System.out.println("Creating InVMNodeManagerServer for ConfluentAmqServer");
    this.nodeManager = nodeManager;
  }

  public InVMNodeManagerServerDelegating(final Configuration configuration,
      final MBeanServer mbeanServer,
      final NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), mbeanServer);
    System.out.println("Creating InVMNodeManagerServer for ConfluentAmqServer");
    this.nodeManager = nodeManager;
  }

  public InVMNodeManagerServerDelegating(final Configuration configuration,
      final ActiveMQSecurityManager securityManager,
      final NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), securityManager);
    System.out.println("Creating InVMNodeManagerServer for ConfluentAmqServer");
    this.nodeManager = nodeManager;
  }

  public InVMNodeManagerServerDelegating(final Configuration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager,
      final NodeManager nodeManager) {
    super(JmsSuiteRunner.wrapConfig(configuration), mbeanServer, securityManager);
    System.out.println("Creating InVMNodeManagerServer for ConfluentAmqServer");
    this.nodeManager = nodeManager;
  }

  protected NodeManager createNodeManager(File directory, boolean replicatingBackup) {
    return nodeManager;
  }
}