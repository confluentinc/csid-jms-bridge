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
import java.util.stream.Collectors;

public final class BridgeConfigFactory {

  private BridgeConfigFactory() {
  }

  public static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(BridgeConfigFactory.class));

  public static Map<String, String> fetchMapConfigWithDefaults(
      String configKey, Config keyConfig, Config defaultMapConfig) {

    if (keyConfig.hasPath(configKey)) {
      return flattenConfig(keyConfig.getConfig(configKey).withFallback(defaultMapConfig));
    } else {
      return flattenConfig(defaultMapConfig);
    }
  }

  public static Map<String, String> flattenConfig(Config config) {
    return config.entrySet()
        .stream()
        .filter(en -> en.getValue() != null)
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            en -> en.getValue().unwrapped().toString()));
  }

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

  public static Properties mapToProps(Map<String, String> propsMap) {
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

  public static BridgeConfig gatherConfiguration(URL configUrl) {
    return loadConfiguration(configUrl).build();
  }

  public static BridgeConfig.Builder loadConfiguration(Path configUrl) {
    try {
      return loadConfiguration(configUrl.toUri().toURL());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static BridgeConfig.Builder loadConfiguration(URL configUrl) {
    Config theirConfig = ConfigFactory.parseURL(configUrl);

    return loadConfiguration(theirConfig);
  }

  public static BridgeConfig.Builder loadConfiguration(String configString) {
    Config theirConfig = ConfigFactory.parseString(configString);

    return loadConfiguration(theirConfig);
  }

  public static String getBridgeVersion() {
    String versionFromManifest = BridgeConfigFactory.class.getPackage().getImplementationVersion();
    return versionFromManifest == null ? "unknown" : versionFromManifest;
  }

  private static BridgeConfig.Builder loadConfiguration(Config appConfig) {
    Config config = appConfig.withFallback(ConfigFactory.defaultReferenceUnresolved()).resolve();

    config.checkValid(ConfigFactory.defaultReference(), "bridge");
    BridgeConfig.Builder bridgeConfig = new BridgeConfig.Builder(config);

    return bridgeConfig;
  }
}
