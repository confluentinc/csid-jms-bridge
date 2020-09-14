/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.Resources;
import com.typesafe.config.ConfigException;
import io.confluent.amq.config.RoutingConfig.Route;
import java.net.URL;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class BridgeConfigFactoryTest {

  @Test
  public void testGatherConfiguration() throws Exception {
    URL testConfig = Resources.getResource("good-config.conf");
    BridgeConfig config = BridgeConfigFactory.gatherConfiguration(testConfig);
    assertEquals("test-bridge", config.id());
    assertEquals(1, config.kafka().size());
    assertEquals(
        "localhost:9092", config.kafka().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(
        "localhost:9092", config.streams().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(4, config.streams().size());
    assertEquals(Duration.ofSeconds(60), config.journals().readyTimeout());
    assertEquals(Duration.ofSeconds(1), config.journals().readyCheckInterval());
  }

  @Test
  public void testBadConfiguration() throws Exception {
    URL testConfig = Resources.getResource("bad-config.conf");
    Exception exception = assertThrows(ConfigException.class, () -> {
      BridgeConfigFactory.gatherConfiguration(testConfig);
    });
    assertTrue(exception.getMessage().contains("'id' is set to null"));
  }

  @Test
  public void testConfigWithRoute() throws Exception {
    URL testConfig = Resources.getResource("route-good-config.conf");
    BridgeConfig config = BridgeConfigFactory.gatherConfiguration(testConfig);
    assertNotNull(config.routing());
    assertFalse(config.routing().deadLetterTopic().isPresent());
    assertEquals(1, config.routing().routes().size());
    Route route = config.routing().routes().get(0);
    assertEquals("my-jms-topic", route.from().address());
    assertEquals("my-kafka-topic", route.to().topic());
  }
}