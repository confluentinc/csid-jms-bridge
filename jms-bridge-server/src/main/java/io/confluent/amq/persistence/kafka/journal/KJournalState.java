/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

public enum KJournalState {
  CREATED,ASSIGNING,LOADING,RUNNING,STOPPED,FAILED;
}
