/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractContainerTest {
  public static final DockerImageName KAFKA_DOCKER_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-kafka:7.3.2-1-ubi8");

  private static final Object READY_LOCK = new Object();
  private static final AtomicInteger SEQUENCE = new AtomicInteger(0);
  private static AdminClient adminClient;
  private static KafkaContainerHelper containerHelper;
  protected static final KafkaContainer KAFKA_CONTAINER;

  static {
    synchronized (READY_LOCK) {
      KAFKA_CONTAINER = new KafkaContainer(KAFKA_DOCKER_IMAGE_NAME)
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
      KAFKA_CONTAINER.start();
      new WaitAllStrategy().waitUntilReady(KAFKA_CONTAINER);
    }
  }

  protected static Properties essentialProps() {
    synchronized (READY_LOCK) {
      Properties props = new Properties();
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          KAFKA_CONTAINER.getBootstrapServers());
      return props;
    }
  }

  protected static synchronized KafkaContainerHelper getContainerHelper() {
    if (adminClient == null) {
      Properties props = new Properties();
      props.setProperty(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
      adminClient = AdminClient.create(props);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        adminClient.close();
      }));
    }

    if (containerHelper == null) {
      containerHelper = new KafkaContainerHelper(KAFKA_CONTAINER, adminClient, SEQUENCE);
    }
    return containerHelper;
  }

  @AfterEach
  public void cleanup() {
    getContainerHelper().closeClients();
  }

  public static void cleanupAll() {
    getContainerHelper().cleanUpTopics();
  }
}

