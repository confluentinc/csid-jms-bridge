/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import org.apache.activemq.artemis.utils.CompositeAddress;
import org.inferred.freebuilder.FreeBuilder;
import org.inferred.freebuilder.IgnoredByEquals;

import io.confluent.amq.config.RoutingConfig.RoutedTopic;

/**
 * Describes the relationship between a single kafka topic and AMQ destination.
 */
@FreeBuilder
public interface KafkaTopicExchange {
  String KAFKA_QUEUE_NAME = "kafka_forward";

  @IgnoredByEquals
  RoutedTopic originConfig();

  String kafkaTopicName();

  String amqAddressName();

  @IgnoredByEquals
  default String ingressQueueName() {
    return CompositeAddress.toFullyQualified(amqAddressName(), kafkaTopicName() + "-" + KAFKA_QUEUE_NAME);
  }

  class Builder extends KafkaTopicExchange_Builder {

  }
}
