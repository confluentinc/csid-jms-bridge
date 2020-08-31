/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import java.util.List;

public interface KJournalListener {

  KJournalListener NO_OP = new KJournalListener() {
    @Override
    public void onRevokedAssignment(List<KJournalAssignment> assignmentList) {

    }

    @Override
    public void onNewAssignment(List<KJournalAssignment> assignmentList) {

    }

    @Override
    public void onStateChange(String journalName, KJournalState oldState,
        KJournalState newState) {

    }
  };

  void onRevokedAssignment(List<KJournalAssignment> assignmentList);

  void onNewAssignment(List<KJournalAssignment> assignmentList);

  void onStateChange(
      String journalName, KJournalState oldState, KJournalState newState);

}
