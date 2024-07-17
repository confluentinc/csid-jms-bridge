/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import static io.confluent.amq.config.BridgeConfigFactory.flattenConfig;

import com.typesafe.config.Config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValue;
import io.kcache.KafkaCacheConfig;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface BridgeConfig {
  Integer MAX_RECORD_SIZE = 1024 * 1024;
  Duration READY_TIMEOUT = Duration.ofMinutes(5);
  Duration READY_CHECK_INTERVAL = Duration.ofSeconds(10);

  String id();

  Optional<String> partnerSFDCId();

  BridgeClientId clientId();

  HaConfig haConfig();

  Map<String, String> kafka();

  JournalsConfig journals();

  Optional<RoutingConfig> routing();

  Optional<SecurityConfig> security();

  class Builder extends BridgeConfig_Builder {

    public Builder() {}

    public Builder(Config rootConfig) {
      Config bridgeConfig = rootConfig.getConfig("bridge");
      Config kafkaConfig = bridgeConfig.getConfig("kafka");
      this.id(bridgeConfig.getString("id"))
          .journals(new JournalsConfig.Builder(bridgeConfig.getConfig("journals"), kafkaConfig))
          .putAllKafka(flattenConfig(kafkaConfig));

      if (bridgeConfig.hasPath("ha")) {
        Config haConfig = bridgeConfig.getConfig("ha");
        this.haConfig(new HaConfig.Builder(bridgeConfig, haConfig));
      }

      if (bridgeConfig.hasPath("routing")) {
        this.routing(
            new RoutingConfig.Builder(
                    bridgeConfig.getConfig("kafka"), bridgeConfig.getConfig("routing"))
                .build());
      }

      if (bridgeConfig.hasPath("security")) {
        this.security(new SecurityConfig.Builder(bridgeConfig.getConfig("security")).build());
      }

      if (bridgeConfig.hasPath(("partner_sfdc_id"))) {
        this.partnerSFDCId(bridgeConfig.getString("partner_sfdc_id"));
      }
      this.clientId(
          new BridgeClientId.Builder()
              .partnerSFDCId(this.partnerSFDCId())
              .bridgeId(this.id())
              .build());
    }
  }

  @FreeBuilder
  interface JournalsConfig {

    int maxMessageSize();

    Duration readyTimeout();

    Duration readyCheckInterval();

    JournalConfig bindings();

    JournalConfig messages();

    class Builder extends BridgeConfig_JournalsConfig_Builder {

      public Builder() {
        this.maxMessageSize(MAX_RECORD_SIZE);
        this.readyTimeout(READY_TIMEOUT);
        this.readyCheckInterval(READY_CHECK_INTERVAL);
      }

      public Builder(Config journalsConfig, Config kafkaConfig) {
        this.maxMessageSize(MAX_RECORD_SIZE);
        this.readyTimeout(READY_TIMEOUT);
        this.readyCheckInterval(READY_CHECK_INTERVAL);

        this.bindings(
            new JournalConfig.Builder(journalsConfig.getConfig("bindings"), kafkaConfig));
        this.messages(
            new JournalConfig.Builder(journalsConfig.getConfig("messages"), kafkaConfig));
      }

      @Override
      public JournalsConfig build() {

        // validate that the group id and topic ids of bindings and messages are not the same
        if (this.bindingsBuilder()
                .getKcacheGroupId()
                .equals(this.messagesBuilder().getKcacheGroupId())) {

          throw new ConfigException.BadValue(
                  "journals.<bindings|messages>.kcache.kafkacache.group.id",
                  String.format(
                          "kafkacache.group.id for bindings and messages should be different. Found %s",
                          this.bindingsBuilder().getKcacheGroupId()));
        }
        if (this.bindingsBuilder().getKcacheTopic().equals(this.messagesBuilder().getKcacheTopic())) {
          throw new ConfigException.BadValue(
                  "journals.<bindings|messages>.kcache.kafkacache.topic.name",
                  String.format(
                          "kafkacache.topic.name for bindings and messages should be different. Found %s",
                          this.bindingsBuilder().getKcacheTopic()));
        }        return super.build();
      }
    }
  }

  @FreeBuilder
  interface JournalConfig {

    Map<String, String> kcache();

    class Builder extends BridgeConfig_JournalConfig_Builder {

      public Builder() {}

      public Builder(Config journalConfig, Config kafkaConfig) {
        this.putAllKcache(
            flattenConfig(journalConfig.getConfig("kcache").withFallback(kafkaConfig)));
        // validate that kafkacache.group.id, kafkacache.topic  is supplied and is not using the
        // default
        if (isKcacheGroupIdNotValid()) {
          throw new ConfigException.BadValue(
              // current path to config
              "journals.<bindings|messages>.kcache.kafkacache.group.id",
              String.format(
                  "kafkacache.group.id is required and not equal to default %s",
                  KafkaCacheConfig.DEFAULT_KAFKACACHE_GROUP_ID_PREFIX + "-" + getDefaultHost()));
        }
        if (isKcacheTopicNotValid()) {
          throw new ConfigException.BadValue(
              "journals.<bindings|messages>.kafkacache.topic.name",
              String.format(
                  "kafkacache.topic is required and not equal to default %s",
                  KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC));
        }
        setKCacheDefaultValues(kafkaConfig);
      }

      private void setKCacheDefaultValues(Config kafkaConfig) {
        // sets the topic here as the default config would conflict with hocons ability to parse
        // nested configs which are decided by the prefix kafkacache.topic
        this.mutateKcache(
            kcache ->
                kcache.put(
                    KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG,
                    kcache().get("kafkacache.topic.name")));
        // set sensible defaults for `segment.bytes` to "134217728" (128MB)
        if (!this.kcache().containsKey("kafkacache.topic.config.segment.bytes")) {
          this.mutateKcache(
              kcache -> kcache.put("kafkacache.topic.config.segment.bytes", "134217728"));
        }
        // iterate through the kafka config and add a prefix of kafkacache. to the kcache config if
        // it does not exist
        kafkaConfig.entrySet().forEach(this::populateKCacheConfigsFromKafkaConfigs);
      }

      private void populateKCacheConfigsFromKafkaConfigs(
          Map.Entry<String, ConfigValue> kafkaConfig) {
        if (!this.kcache().containsKey("kafkacache." + kafkaConfig.getKey())) {
          this.mutateKcache(
              kcache ->
                  kcache.put(
                      "kafkacache." + kafkaConfig.getKey(),
                      kafkaConfig.getValue().unwrapped().toString()));
        }
      }

      private boolean isKcacheTopicNotValid() {
        return !(this.kcache().containsKey(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG + ".name")
            && !this.kcache()
                .get(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG + ".name")
                .equals(KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC));
      }

      private boolean isKcacheGroupIdNotValid() {
        return !(this.kcache().containsKey(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG)
            && !this.kcache()
                .get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG)
                .equals(
                    KafkaCacheConfig.DEFAULT_KAFKACACHE_GROUP_ID_PREFIX + "-" + getDefaultHost()));
      }

      private String getKcacheGroupId() {
        return this.kcache().get(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG);
      }

      private String getKcacheTopic() {
        return this.kcache().get(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG);
      }

      private static String getDefaultHost() {
        try {
          return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException("Unable to determine hostname", e);
        }
      }
    }
  }
}
