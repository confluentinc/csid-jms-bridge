/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

public final class StreamsSupport {

  private StreamsSupport() {
  }

  /**
   * Apply given functions to the stream when the predicate is met. Each predicate should be
   * mutually exclusive since the first one passing will be the one used.
   * <p>
   * Returns the stream of entries that do not match any of the given predicates.
   * </p><p>
   * See {@link KStream#branch(Predicate[])}.
   * </p>
   *
   * @param stream the KStream to branch from
   * @param sb     used to add stores to the topology if needed.
   * @return the stream of entries that do not match any of the given predicate executions.
   */
  public static <K, V> BranchSupport.Spec<K, V> branchStream(
      KStream<K, V> stream,
      StreamsBuilder sb) {

    return new BranchSupport<>(stream, sb).specify();
  }

  public static <K, V> BranchSupport.Spec<K, V> branchStream(KStream<K, V> stream) {
    return branchStream(stream, null);
  }
}

