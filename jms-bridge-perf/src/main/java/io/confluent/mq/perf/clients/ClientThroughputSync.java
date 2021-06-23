/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.base.Stopwatch;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class ClientThroughputSync {

  private final Semaphore throughput;
  private final CountDownLatch starter;
  private final CountDownLatch stopper;


  public ClientThroughputSync(int parties, int initialThroughput) {
    this.starter = new CountDownLatch(parties);
    this.stopper = new CountDownLatch(parties);
    this.throughput = new Semaphore(initialThroughput);
  }

  public ClientThroughputSync(int parties) {
    this(parties, 0);
  }

  public void signalReady() {
    starter.countDown();
  }

  public void signalComplete() {
    stopper.countDown();
  }

  public boolean awaitReady(Duration maxWaitTime) throws InterruptedException {
    return starter.await(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
  }

  public boolean awaitComplete(Duration maxWaitTime) throws InterruptedException {
    return stopper.await(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void increment(int count) {
    this.throughput.release(count);
  }

  public void increment() {
    this.increment(1);
  }

  public boolean awaitNext(Duration maxWaitTime) throws InterruptedException {
    return awaitNext(1, maxWaitTime);
  }

  public boolean awaitNext(int count, Duration maxWaitTime) throws InterruptedException {
    return this.throughput.tryAcquire(count, maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
  }

  public boolean awaitEmpty(Duration maxWaitTime) throws InterruptedException {

    int sleepyTime = Math.min(5000, Math.max(100, (int) maxWaitTime.getSeconds() * 100));
    Stopwatch stopwatch = Stopwatch.createStarted();
    boolean timeRemains = true;
    while (timeRemains) {
      if (this.throughput.tryAcquire()) {
        this.throughput.release();
        Thread.sleep(sleepyTime);
        timeRemains = stopwatch.elapsed().minus(maxWaitTime).isNegative();
      } else {
        return true;
      }
    }
    return false;
  }
}
