package io.confluent.amq.config;

import com.typesafe.config.Config;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Map;
import java.util.Optional;

import static io.confluent.amq.config.BridgeConfigFactory.flattenConfig;

@FreeBuilder
public interface HaConfig {
    int DEFAULT_INIT_TIMEOUT_MS = 30_000;
    String DEFAULT_GROUP_ID = "ha_consumer_group";

    String groupId();

    Map<String, Object> consumerConfig();

    int initTimeoutMs();

    class Builder extends HaConfig_Builder {

        public Builder() {
            this.initTimeoutMs(DEFAULT_INIT_TIMEOUT_MS);
        }

        public Builder(Config bridgeConfig, Config haConfig) {
//            Config bridgeConfig = rootConfig.getConfig("bridge");
            Config kafkaConfig = bridgeConfig.getConfig("kafka");
            this.putAllConsumerConfig(BridgeConfigFactory
                    .fetchMapConfigWithDefaults("consumerConfig", haConfig, kafkaConfig));

            if (haConfig.hasPath("init.timeout.ms")) {
                this.initTimeoutMs(haConfig.getInt("init.timeout.ms"));
            } else {
                this.initTimeoutMs(DEFAULT_INIT_TIMEOUT_MS);
            }

            if (haConfig.hasPath("group.id")) {
                this.groupId(generateGroupId(bridgeConfig.getString("id"), haConfig.getString("group.id")));
            } else {
                this.groupId(generateGroupId(bridgeConfig.getString("id")));
            }
        }

        String generateGroupId(String bridgeId) {
            return generateGroupId(bridgeId, DEFAULT_GROUP_ID);
        }

        String generateGroupId(String bridgeId, String groupId) {
            return bridgeId + "_" + groupId;
        }
    }
}
