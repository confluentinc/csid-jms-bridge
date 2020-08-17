/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.kafka.ReconciledMessage;
import java.util.Collections;
import java.util.List;

public interface KafkaJournalHandler {
  KafkaJournalHandler NO_OP = (k, r) -> Collections.emptyList();

  List<ReconciledMessage> handleRecord(JournalEntryKey key, JournalRecord record);

}
