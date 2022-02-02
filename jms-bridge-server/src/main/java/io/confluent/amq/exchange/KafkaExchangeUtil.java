/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.config.BridgeConfig;

public final class KafkaExchangeUtil {

  private KafkaExchangeUtil() {
  }

  public static String createIngressDestinationName(String topicAddress) {
    return String.format("%s::kafka_forward", topicAddress);
  }

  public static String createDivertName(String address) {
    return String.format("%s.to-kafka-divert", address);
  }

  public static String createConsumerGroupId(BridgeConfig bridgeConfig) {
    return String.format("%s.exchange", bridgeConfig.id());
  }
}
