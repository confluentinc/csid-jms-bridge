/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import io.confluent.csid.common.utils.accelerator.Accelerator;
import io.confluent.csid.common.utils.accelerator.ClientId;
import io.confluent.csid.common.utils.accelerator.Owner;
import org.inferred.freebuilder.FreeBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static io.confluent.amq.config.BridgeConfigFactory.flattenConfig;
import static io.confluent.amq.config.BridgeConfigFactory.getBridgeVersion;

@FreeBuilder
public interface BridgeConfig {

  String id();

  Optional<String> partnerSFDCId();

  BridgeClientId clientId();

  HaConfig haConfig();

  Map<String, String> kafka();

  Map<String, String> streams();

  JournalsConfig journals();

  Optional<RoutingConfig> routing();

  Optional<SecurityConfig> security();

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

      if (bridgeConfig.hasPath("ha")) {
        Config haConfig = bridgeConfig.getConfig("ha");
        this.haConfig(new HaConfig.Builder(bridgeConfig, haConfig));
      }

      if (bridgeConfig.hasPath("routing")) {
        this.routing(new RoutingConfig.Builder(
            bridgeConfig.getConfig("kafka"),
            bridgeConfig.getConfig("routing")).build());
      }

      if (bridgeConfig.hasPath("security")) {
        this.security(new SecurityConfig.Builder(bridgeConfig.getConfig("security")).build());
      }

      if (bridgeConfig.hasPath(("partner_sfdc_id"))) {
        this.partnerSFDCId(bridgeConfig.getString("partner_sfdc_id"));
      }
      this.clientId(new BridgeClientId.Builder()
                      .partnerSFDCId(this.partnerSFDCId())
                      .bridgeId(this.id())
                      .build());
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
