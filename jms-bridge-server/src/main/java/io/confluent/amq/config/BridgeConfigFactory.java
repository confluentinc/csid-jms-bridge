/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.amq.logging.StructuredLogger;
import java.net.URL;

public class BridgeConfigFactory {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(BridgeConfigFactory.class));

  public static BridgeConfig gatherConfiguration(URL configUrl) throws Exception {
    Config theirConfig = ConfigFactory.parseURL(configUrl);
    Config config = theirConfig.withFallback(ConfigFactory.defaultReference()).resolve();

    config.checkValid(ConfigFactory.defaultReference(), "bridge");
    BridgeConfig bridgeConfig = new BridgeConfig.Builder(config).build();

    return bridgeConfig;
  }

}
