package io.confluent.amq.persistence.kafka.journal;

import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface JournalSpec {

  String journalName();

  TopicSpec journalWalTopic();

  TopicSpec journalTableTopic();

  boolean performRouting();

  class Builder extends JournalSpec_Builder {

    public Builder() {
      this.performRouting(false);
    }
  }
}
