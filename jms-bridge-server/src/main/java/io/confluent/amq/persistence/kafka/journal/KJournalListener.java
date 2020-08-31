/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import java.util.List;

public interface KJournalListener {

  KJournalListener NO_OP = new KJournalListener() {
    @Override
    public void onAssignmentChange(KJournalMetadata metadata,
        List<KJournalAssignment> newAssignmentList) {
      //
    }

    @Override
    public void onStateChange(KJournalMetadata metadata, KJournalState oldState,
        KJournalState newState) {
        //
    }
  };

  void onAssignmentChange(KJournalMetadata metadata, List<KJournalAssignment> newAssignmentList);

  void onStateChange(
      KJournalMetadata metadata, KJournalState oldState, KJournalState newState);

}
