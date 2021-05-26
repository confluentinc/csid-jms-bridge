/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Starter implements Runnable {

  private final CompletableFuture<Void> readySignal;
  private final Consumer<CompletableFuture<Void>> runner;

  public Starter(CompletableFuture<Void> readySignal, Consumer<CompletableFuture<Void>> runner) {
    this.readySignal = readySignal;
    this.runner = runner;
  }

  @Override
  public void run() {
    runner.accept(readySignal);
  }

  public CompletableFuture<Void> getReadySignal() {
    return readySignal;
  }
}
