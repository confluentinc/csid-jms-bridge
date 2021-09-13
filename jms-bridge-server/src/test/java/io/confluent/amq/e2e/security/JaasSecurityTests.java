/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.e2e.security;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.confluent.amq.test.KafkaTestContainer;

public class JaasSecurityTests {
  //start kafka broker
  //create test resources, broker.xml, jms-bridge.conf, login.conf, user/roles property files
  //start from JmsBridgeMain

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = KafkaTestContainer.usingDefaults();

  public void testUsersAreFoundForDomain() throws Exception {

  }

  public void testRolesAreAppliedToUsers() throws Exception {

  }
}
