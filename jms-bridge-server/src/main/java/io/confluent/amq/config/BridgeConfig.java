/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import org.inferred.freebuilder.FreeBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static io.confluent.amq.config.BridgeConfigFactory.flattenConfig;

@FreeBuilder
public interface BridgeConfig {

  String id();

  Map<String, String> kafka();

  Map<String, String> streams();

  JournalsConfig journals();

  Optional<RoutingConfig> routing();

  class Builder extends BridgeConfig_Builder {

    public Builder() {

    }

    public Builder(Config rootConfig) {
      Config bridgeConfig = rootConfig.getConfig("bridge");
      this.id(bridgeConfig.getString("id"))
          .journals(new JournalsConfig.Builder(bridgeConfig.getConfig("journals")))
          .putAllKafka(flattenConfig(bridgeConfig.getConfig("kafka")))
          .putAllStreams(flattenConfig(
              bridgeConfig.getConfig("streams")
                  .withFallback(bridgeConfig.getConfig("kafka"))));

      if (bridgeConfig.hasPath("routing")) {
        this.routing(new RoutingConfig.Builder(bridgeConfig.getConfig("routing")).build());
      }
    }

  }

  @FreeBuilder
  interface JournalsConfig {

    int maxMessageSize();

    TopicConfig topic();

    Duration readyTimeout();

    Duration readyCheckInterval();

    JournalConfig bindings();

    JournalConfig messages();

    class Builder extends BridgeConfig_JournalsConfig_Builder {

      public Builder() {

      }

      public Builder(Config journalsConfig) {
        this.bindings(new JournalConfig.Builder(journalsConfig.getConfig("bindings")))
            .maxMessageSize(journalsConfig.getBytes("max.message.size").intValue())
            .messages(new JournalConfig.Builder(journalsConfig.getConfig("messages")))
            .readyTimeout(journalsConfig.getDuration("ready.timeout"))
            .readyCheckInterval(journalsConfig.getDuration("ready.check.interval"))
            .topic(new TopicConfig.Builder(journalsConfig.getConfig("topic")));
      }
    }
  }

  @FreeBuilder
  interface JournalConfig {

    TopicConfig walTopic();

    TopicConfig tableTopic();

    class Builder extends BridgeConfig_JournalConfig_Builder {

      public Builder() {

      }

      public Builder(Config journalConfig) {
        this.walTopic(new TopicConfig.Builder(journalConfig.getConfig("wal.topic")));
        this.tableTopic(new TopicConfig.Builder(journalConfig.getConfig("table.topic")));
      }

    }
  }

  @FreeBuilder()
  interface TopicConfig {

    Optional<String> name();

    int replication();

    int partitions();

    Map<String, Object> options();

    class Builder extends BridgeConfig_TopicConfig_Builder {

      public Builder() {

      }

      public Builder(Config topicConfig) {
        this.replication(topicConfig.getInt("replication"))
            .partitions(topicConfig.getInt("partitions"))
            .putAllOptions(flattenConfig(topicConfig.getConfig("options")));

        if (topicConfig.hasPath("name")) {
          this.name(topicConfig.getString("name"));
        }

      }
    }
  }
}
