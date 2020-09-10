/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import io.confluent.amq.config.RoutingConfig.Builder;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface BridgeConfig {

  String id();

  Map<String, Object> kafka();

  Map<String, Object> streams();

  JournalsConfig journals();

  RoutingConfig routing();

  class Builder extends BridgeConfig_Builder {

    public Builder() {

    }

    public Builder(Config rootConfig) {
      Config bridgeConfig = rootConfig.getConfig("bridge");
      this.id(bridgeConfig.getString("id"))
          .journals(new JournalsConfig.Builder(bridgeConfig.getConfig("journals")))
          .putAllKafka(bridgeConfig.getObject("kafka").unwrapped())
          .putAllStreams(bridgeConfig.getObject("streams").unwrapped());

      if (bridgeConfig.hasPath("routing")) {
        this.routing(new RoutingConfig.Builder(bridgeConfig.getConfig("routing")));
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
            .maxMessageSize(journalsConfig.getBytes("max-message-size").intValue())
            .messages(new JournalConfig.Builder(journalsConfig.getConfig("messages")))
            .readyTimeout(journalsConfig.getDuration("ready-timeout"))
            .readyCheckInterval(journalsConfig.getDuration("ready-check-interval"))
            .topic(new TopicConfig.Builder(journalsConfig.getConfig("topic")));
      }
    }
  }

  @FreeBuilder
  interface JournalConfig {

    TopicConfig topic();

    class Builder extends BridgeConfig_JournalConfig_Builder {
      public Builder() {

      }

      public Builder(Config journalConfig) {
        this.topic(new TopicConfig.Builder(journalConfig.getConfig("topic")));
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
            .putAllOptions(topicConfig.getObject("options").unwrapped());

        if (topicConfig.hasPath("name")) {
          this.name(topicConfig.getString("name"));
        }

      }
    }
  }
}
