package io.confluent.amq.persistence.kafka.journal;

import org.inferred.freebuilder.FreeBuilder;

import java.util.Map;

@FreeBuilder
public interface TopicSpec {

  String name();

  int partitions();

  int replication();

  Map<String, String> configs();

  class Builder extends TopicSpec_Builder {}
}
