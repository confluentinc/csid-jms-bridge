/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.amq.logging.StructuredLogger;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class BridgeConfigFactory {

  private BridgeConfigFactory() {
  }

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(BridgeConfigFactory.class));

  public static Map<String, Object> propsToMap(Properties props) {
    Map<String, Object> propsMap = new HashMap<>();
    props.forEach((k, v) -> propsMap.put(k.toString(), Objects.toString(v)));
    return propsMap;
  }

  public static Properties propsToMap(Map<String, Object> propsMap) {
    Properties props = new Properties();
    propsMap.forEach((k, v) -> props.put(k, Objects.toString(v)));
    return props;
  }

  public static BridgeConfig gatherConfiguration(Path configUrl) {
    try {
      return gatherConfiguration(configUrl.toUri().toURL());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static BridgeConfig gatherConfiguration(URL configUrl) throws Exception {
    Config theirConfig = ConfigFactory.parseURL(configUrl);
    Config config = theirConfig.withFallback(ConfigFactory.defaultReference()).resolve();

    config.checkValid(ConfigFactory.defaultReference(), "bridge");
    BridgeConfig bridgeConfig = new BridgeConfig.Builder(config).build();

    return bridgeConfig;
  }

}
