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
import io.kcache.KafkaCacheConfig;
import java.net.URL;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BridgeConfigFactoryTest {

  @Test
  public void testGatherConfiguration() throws Exception {
    BridgeConfig config = getConfig("good-config.conf");

    assertEquals("test-bridge", config.id());
    assertEquals(1, config.kafka().size());
    assertEquals("localhost:9092", config.kafka().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

    assertEquals(
        "_test-bridge-bindings-kcache",
        config.journals().bindings().kcache().get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG));
    assertEquals(
        "_test-bridge-messages-kcache",
        config.journals().messages().kcache().get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG));
    assertEquals(
        "_test-bridge-bindings-kcache",
        config.journals().bindings().kcache().get(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG));
    assertEquals(
        "_test-bridge-messages-kcache",
        config.journals().messages().kcache().get(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG));
    assertEquals(
        "134217728",
        config.journals().messages().kcache().get("kafkacache.topic.config.segment.bytes"));
    assertEquals(
        "134217728",
        config.journals().bindings().kcache().get("kafkacache.topic.config.segment.bytes"));

    assertEquals(Duration.ofSeconds(60), config.journals().readyTimeout());
    assertEquals(Duration.ofSeconds(1), config.journals().readyCheckInterval());
  }

  @Test
  public void testBadConfiguration() throws Exception {
    Exception exception =
        assertThrows(
            ConfigException.class,
            () -> {
              getConfig("bad-config.conf");
            });
    assertTrue(exception.getMessage().contains("'id' is set to null"));
  }

  @Test
  public void testBadConfigurationWithSameKcacheGroupId() throws Exception {
    Exception exception =
        assertThrows(
            ConfigException.class,
            () -> {
              getConfig("bad-config-same-kcache-group-id.conf");
            });
    System.out.println(exception.getMessage());
    assertTrue(
        exception
            .getMessage()
            .contains(
                "kafkacache.group.id for bindings and messages should be different. Found _blah"));
  }

  @Test
  public void testBadConfigurationWithSameKcacheTopicName() throws Exception {
    Exception exception =
        assertThrows(
            ConfigException.class,
            () -> {
              getConfig("bad-config-same-kcache-topic-name.conf");
            });
    System.out.println(exception.getMessage());
    assertTrue(
        exception
            .getMessage()
            .contains(
                "kafkacache.topic.name for bindings and messages should be different. Found _blah"));
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
    assertEquals(
        routedTopicDefaults.correlationKeyOverride(), routedTopic.correlationKeyOverride());
  }

  @ParameterizedTest
  @ValueSource(strings = {"route-good-config.conf", "route-good-config.properties"})
  public void testMultipleRoutingRulesConfig(String configFile) throws Exception {
    BridgeConfig config = getConfig(configFile);
    assertTrue(config.routing().isPresent());

    RoutingConfig routingConfig = config.routing().get();
    assertEquals(2, routingConfig.topics().size());

    RoutedTopic routedTopic1 = routingConfig.topics().get(0);
    assertEquals("quick-start-request", routedTopic1.match());
    assertEquals("TEXT", routedTopic1.messageType());

    RoutedTopic routedTopic2 = routingConfig.topics().get(1);
    assertEquals("quick-start-response", routedTopic2.match());
    assertEquals("TEXT", routedTopic2.messageType());
    assertTrue(routedTopic2.consumeAlways());
  }

  @Test
  public void testGetBridgeVersion() {
    String version = BridgeConfigFactory.getBridgeVersion();
    System.out.println("version: " + version);
    assertEquals("unknown", version);
  }

  private BridgeConfig getConfig(String configName) {
    URL testConfig = Resources.getResource("config/" + configName);
    return BridgeConfigFactory.gatherConfiguration(testConfig);
  }
}
