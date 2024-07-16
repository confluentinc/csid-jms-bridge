package io.confluent.amq.persistence.kafka.journal;

import org.inferred.freebuilder.FreeBuilder;

import java.util.Map;

@FreeBuilder
public interface JournalSpec {

  String journalName();

  Map<String,String> kcacheConfig();

  boolean performRouting();

  class Builder extends JournalSpec_Builder {

    public Builder() {
      this.performRouting(false);
    }
  }
}
