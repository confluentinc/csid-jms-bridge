/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public enum KJournalState {
  CREATED(false, asList("STARTED", "STOPPED", "STOPPING", "FAILED")),
  STARTED(false, asList("ASSIGNING", "STOPPED", "STOPPING", "FAILED")),
  ASSIGNING(true, asList("LOADING", "RUNNING", "STOPPING", "STOPPED", "FAILED")),
  LOADING(true, asList("ASSIGNING", "RUNNING", "STOPPING", "STOPPED", "FAILED")),
  RUNNING(true, asList("ASSIGNING", "LOADING", "STOPPING", "STOPPED", "FAILED")),
  STOPPING(false, asList("FAILED", "STARTED", "STOPPING")),
  STOPPED(false, asList("FAILED", "STARTED")),
  FAILED(false, Collections.emptyList());

  private final boolean isRunningState;
  private final Set<String> validTransitions;

  KJournalState(boolean isRunningState, List<String> validTransitions) {
    this.isRunningState = isRunningState;
    this.validTransitions = new HashSet<>(validTransitions);
    this.validTransitions.add(this.name());
  }

  public boolean isRunningState() {
    return isRunningState;
  }

  public boolean validTransition(KJournalState otherState) {
    return otherState == this || validTransitions.contains(otherState.name());
  }
}

