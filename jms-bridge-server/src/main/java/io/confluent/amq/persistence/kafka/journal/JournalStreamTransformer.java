/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;

public abstract class JournalStreamTransformer extends
    BaseJournalStreamTransformer<JournalEntryKey, JournalEntry> {

  public JournalStreamTransformer(String journalName, String storeName) {
    super(journalName, storeName);
  }
}
