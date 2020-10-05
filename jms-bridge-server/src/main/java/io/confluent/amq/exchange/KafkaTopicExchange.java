/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import org.inferred.freebuilder.FreeBuilder;
import org.inferred.freebuilder.IgnoredByEquals;

/**
 * Describes the relationship between a single kafka topic and AMQ destination.
 */
@FreeBuilder
public interface KafkaTopicExchange {

  @IgnoredByEquals
  RoutedTopic originConfig();

  String kafkaTopicName();

  String amqAddressName();

  @IgnoredByEquals
  String ingressQueueName();

  class Builder extends KafkaTopicExchange_Builder {

  }
}
