/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface KJournalAssignment {

  String journalName();

  int partition();

  class Builder extends KJournalAssignment_Builder {

  }
}
