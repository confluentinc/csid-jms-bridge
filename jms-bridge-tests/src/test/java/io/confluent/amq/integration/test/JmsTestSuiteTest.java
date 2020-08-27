/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.testcontainers.containers.KafkaContainer;

@RunWith(JmsSuiteRunner.class)
public class JmsTestSuiteTest {

  private JmsTestSuiteTest() {
  }

  public static AtomicInteger BRIDGE_ID_SEQUENCE = new AtomicInteger(1);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static KafkaContainer kafkaContainer =
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");


}
