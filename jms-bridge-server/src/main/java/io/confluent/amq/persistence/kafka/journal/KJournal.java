/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoader;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoaderCallback;

public interface KJournal {

  default String prefix(String subject) {
    return storeName() + "_" + subject;
  }

  default String txStoreName() {
    return storeName() + "_tx";
  }

  String name();

  String walTopic();

  String tableTopic();

  String storeName();

  void stop();

  void load(KafkaJournalLoaderCallback callback);

  KafkaJournalLoader loader();

  boolean isAssignedPartition(int partition);

  boolean isRunning();
}
