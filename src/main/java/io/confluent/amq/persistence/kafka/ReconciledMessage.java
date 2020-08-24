/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface ReconciledMessage {

  boolean isTombstoned();

  boolean isDeadLettered();

  boolean isForwarded();

  Optional<DeadLetter> getDeadLetter();

  Optional<Tombstone> getTombstone();

  Optional<Forward> getForward();

  interface ReconciledKey {

    JournalEntryKey getKey();
  }

  interface ReconciledEntry extends ReconciledKey {

    JournalEntry getValue();
  }

  @FreeBuilder
  interface DeadLetter extends ReconciledEntry {

    String getReasonCode();

    String getReason();

    class Builder extends ReconciledMessage_DeadLetter_Builder {

    }
  }

  @FreeBuilder
  interface Tombstone extends ReconciledKey {
    class Builder extends ReconciledMessage_Tombstone_Builder {

    }
  }

  @FreeBuilder
  interface Forward extends ReconciledEntry {
    class Builder extends ReconciledMessage_Forward_Builder {

    }
  }

  static ReconciledMessage tombstone(JournalEntryKey key) {
    return new Builder()
        .setTombstoned(true)
        .setTombstone(new Tombstone.Builder().setKey(key).build())
        .build();
  }

  static ReconciledMessage deadletter(
      JournalEntryKey key,
      JournalEntry value,
      String reasonCode,
      String reasonDesc) {

    return new Builder()
        .setDeadLettered(true)
        .setDeadLetter(new DeadLetter.Builder()
            .setReason(reasonDesc)
            .setReasonCode(reasonCode)
            .setKey(key)
            .setValue(value)
            .build())
        .build();
  }

  static ReconciledMessage forward(JournalEntryKey key, JournalEntry value) {
    return new Builder()
        .setForwarded(true)
        .setForward(new Forward.Builder()
          .setKey(key)
          .setValue(value)
          .build())
        .build();
  }


  class Builder extends ReconciledMessage_Builder {

    public Builder() {
      this.setTombstoned(false)
          .setDeadLettered(false)
          .setForwarded(false);
    }
  }
}

