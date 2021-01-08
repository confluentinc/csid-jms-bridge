/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.streams;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

public class BranchSupport<K, V> {

  private final KStream<K, V> stream;
  private final StreamsBuilder streamsBuilder;

  /**
   * Apply given functions to the stream when the predicate is met. Each predicate should be
   * mutually exclusive since the first one passing will be the one used.
   * <p>
   * Returns the stream of entries that do not match any of the given predicates.
   * </p><p>
   * See {@link KStream#branch(Predicate[])}.
   * </p>
   *
   * @param stream         the KStream to branch from
   * @param streamsBuilder used to add stores to the topology if needed.
   * @return the stream of entries that do not match any of the given predicate executions.
   */

  public BranchSupport(KStream<K, V> stream,
      StreamsBuilder streamsBuilder) {
    this.stream = stream;
    this.streamsBuilder = streamsBuilder;
  }

  public Spec<K, V> specify() {
    return new Spec<>(stream);
  }

  public interface BranchSpec<K, V> {

    BranchStreamSpec<K, V> branchTo(Consumer<KStream<K, V>> executor);
  }

  public interface BranchStreamSpec<K, V> {

    BranchSpec<K, V> when(Predicate<K, V> predicate);

    KStream<K, V> done();
  }

  public static class Spec<K, V> implements
      BranchSpec<K, V>, BranchStreamSpec<K, V> {


    private final List<BranchExecution<K, V>> execs = new ArrayList<>();
    private final KStream<K, V> stream;
    private Predicate<K, V> currPredicate;

    Spec(KStream<K, V> stream) {
      this.stream = stream;
    }

    @Override
    public BranchStreamSpec<K, V> branchTo(Consumer<KStream<K, V>> executor) {
      Preconditions.checkState(currPredicate != null,
          "When must be called prior to branchTo with a non-null predicate.");

      Preconditions.checkArgument(executor != null,
          "branchTo requires a non-null execution target.");

      execs.add(new BranchExecution<>(currPredicate, executor));
      currPredicate = null;

      return this;
    }

    @Override
    public BranchSpec<K, V> when(Predicate<K, V> predicate) {
      currPredicate = predicate;
      return this;
    }

    @Override
    public KStream<K, V> done() {
      if (execs.isEmpty()) {
        return stream;
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      Predicate<K, V>[] predicates =
          new Predicate[execs.size() + 1];

      for (int i = 0; i < execs.size(); i++) {
        predicates[i] = execs.get(i).predicate;
      }
      predicates[execs.size()] = (k, v) -> true;

      KStream<K, V>[] branches = stream
          .branch(predicates);

      for (int i = 0; i < execs.size(); i++) {
        execs.get(i).execution.accept(branches[i]);
      }
      return branches[execs.size()];
    }
  }

  static class BranchExecution<K, V> {

    final Predicate<K, V> predicate;
    final Consumer<KStream<K, V>> execution;

    BranchExecution(
        Predicate<K, V> predicate,
        Consumer<KStream<K, V>> execution) {

      this.predicate = predicate;
      this.execution = execution;
    }
  }
}
