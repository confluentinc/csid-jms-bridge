/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.Resources;
import com.typesafe.config.ConfigException;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import java.net.URL;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class BridgeConfigFactoryTest {

  @Test
  public void testGatherConfiguration() throws Exception {
    BridgeConfig config = getConfig("good-config.conf");

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
    Exception exception = assertThrows(ConfigException.class, () -> {
      getConfig("bad-config.conf");
    });
    assertTrue(exception.getMessage().contains("'id' is set to null"));
  }

  @Test
  public void testMinimalConfiguration() throws Exception {
    BridgeConfig config = getConfig("minimal.conf");
    assertEquals("minimal", config.id());
  }

  @Test
  public void testMinimumRoutingConfig() throws Exception {
    RoutingConfig routingConfigDefaults = new RoutingConfig.Builder().buildPartial();

    BridgeConfig config = getConfig("minimal-routing.conf");
    assertTrue(config.routing().isPresent());

    RoutingConfig routingConfig = config.routing().get();
    assertEquals(1, routingConfig.topics().size());
    assertEquals(routingConfigDefaults.metadataRefreshMs(), routingConfig.metadataRefreshMs());

    RoutedTopic routedTopicDefaults = new RoutedTopic.Builder().buildPartial();
    RoutedTopic routedTopic = routingConfig.topics().get(0);
    assertEquals("[^_].*", routedTopic.match());
    assertEquals(routedTopicDefaults.addressTemplate(), routedTopic.addressTemplate());
    assertEquals(routedTopicDefaults.keyProperty(), routedTopic.keyProperty());
    assertEquals(routedTopicDefaults.messageType(), routedTopic.messageType());
    assertEquals(routedTopicDefaults.correlationKeyOverride(),
        routedTopic.correlationKeyOverride());

  }


  private BridgeConfig getConfig(String configName) {
    URL testConfig = Resources.getResource("config/" + configName);
    return BridgeConfigFactory.gatherConfiguration(testConfig);

  }
}