/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoaderCallback;

public interface KJournal {

  String name();

  String topic();

  String storeName();

  void stop();

  void load(KafkaJournalLoaderCallback callback);

  boolean isAssignedPartition(int partition);

  boolean isRunning();
}
