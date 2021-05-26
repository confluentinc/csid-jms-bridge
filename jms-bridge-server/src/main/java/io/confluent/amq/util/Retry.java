/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.util;

import java.time.Duration;

/**
 * Quick class used to retry functions.
 */
public class Retry {

  private final int maxAttempts;
  private final Duration maxTime;
  private final Duration attemptDelay;

  public Retry(int maxAttempts, Duration maxTime, Duration attemptDelay) {
    this.maxAttempts = maxAttempts - 1;
    this.maxTime = maxTime;
    this.attemptDelay = attemptDelay;
  }

  public <RT> RT retry(Action<RT> action, AttemptEvaluator<RT> retryCondition) {
    long startNanos = System.nanoTime();
    int attempts = -1;
    Exception caughtException = null;
    RT result = null;

    while (
        attempts < maxAttempts
            && !maxTime.minusNanos(System.nanoTime() - startNanos).isNegative()) {

      try {
        result = action.execute();
        caughtException = null;
      } catch (Exception e) {
        caughtException = e;
        result = null;
      }

      if (!retryCondition.tryAgain(result, caughtException)) {
        attempts = Integer.MAX_VALUE;
      } else {
        try {
          Thread.sleep(attemptDelay.toMillis());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        attempts++;
      }
    }

    if (caughtException != null) {
      throw new RuntimeException(caughtException);
    }
    return result;
  }

  public interface Action<T> {

    T execute() throws Exception;
  }

  public interface AttemptEvaluator<T> {

    boolean tryAgain(T result, Throwable err);
  }
}
