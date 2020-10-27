/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.logging.StructuredLogger;
import java.util.Optional;

public class ComponentLifeCycle {

  private final StructuredLogger logger;
  private volatile State state = State.CREATED;

  public ComponentLifeCycle(StructuredLogger logger) {
    this.logger = logger;
  }

  public boolean isPrepared() {
    return state == State.PREPARED;
  }

  public boolean isStopped() {
    return state == State.STOPPED;
  }

  public boolean isStarted() {
    return state == State.STARTED;
  }

  public boolean isCreated() {
    return state == State.CREATED;
  }

  public TransitionResult prepare() {
    return doTransition(State.PREPARED);
  }

  public TransitionResult start() {
    return doTransition(State.STARTED);
  }

  public TransitionResult stop() {
    return doTransition(State.STOPPED);
  }

  public TransitionResult doPrepare(Runnable activity) {
    return doTransition(State.PREPARED, activity);
  }

  public TransitionResult doStart(Runnable activity) {
    return doTransition(State.STARTED, activity);
  }

  public TransitionResult doStop(Runnable activity) {
    return doTransition(State.STOPPED, activity);
  }

  public State getState() {
    return state;
  }

  private synchronized TransitionResult doTransition(State nextState) {
    return doTransition(nextState, () -> {
      //do nothing
    });
  }

  private synchronized TransitionResult doTransition(State nextState, Runnable activity) {
    TransitionResult tr = null;
    logger.info(b -> b.event(loggingEvent(nextState)));

    if (state == nextState) {
      //no transition needed, already at desired state
      tr = new TransitionResult(true, null, null);
    } else if (isLegalTransition(nextState)) {
      activity.run();
      state = nextState;
      tr = new TransitionResult(true, null, null);
    } else {

      tr = new TransitionResult(
          false,
          String.format("Invalid state transition %s -> %s", state, nextState),
          null);
    }

    final TransitionResult finalResult = tr;
    if (finalResult.isSuccess()) {
      logger.info(b -> b.event(loggingEvent(nextState)).markCompleted());
    } else {
      logger.info(b -> {
        b.event(loggingEvent(nextState))
            .markFailure();
        finalResult.getError().ifPresent(b::error);
        finalResult.getInvalidTransitionMessage().ifPresent(b::message);
      });
    }

    return finalResult;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public boolean isLegalTransition(State nextState) {
    switch (nextState) {
      case CREATED:
        return false;
      case PREPARED:
        return state == State.CREATED || state == State.STOPPED;
      case STARTED:
        return state == State.STOPPED || state == State.PREPARED;
      case STOPPED:
        return state == State.STARTED;
      default:
        return false;
    }
  }

  private String loggingEvent(State state) {
    switch (state) {
      case CREATED:
        return "Creating";
      case PREPARED:
        return "Preparing";
      case STARTED:
        return "Starting";
      case STOPPED:
        return "Stopping";
      default:
        return "DoingUnknown";
    }
  }

  public static class TransitionResult {

    private final boolean success;
    private final String invalidTransitionMessage;
    private final Throwable error;

    public TransitionResult(boolean success, String invalidTransitionMessage, Throwable error) {
      this.success = success;
      this.invalidTransitionMessage = invalidTransitionMessage;
      this.error = error;
    }

    public boolean isSuccess() {
      return success;
    }

    public Optional<String> getInvalidTransitionMessage() {
      return Optional.ofNullable(invalidTransitionMessage);
    }

    public Optional<Throwable> getError() {
      return Optional.ofNullable(error);
    }
  }

  public enum State {
    CREATED, PREPARED, STARTED, STOPPED
  }
}
