/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface KJournalMetadata {
  String nodeId();

  String journalName();

  class Builder extends KJournalMetadata_Builder {

  }
}
