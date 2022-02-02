/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import com.google.common.collect.ImmutableSet;
import io.confluent.amq.logging.StructuredLogger;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Contains information on the configuration of how data is exchanged between kakfa topics and amq
 * destinations.
 */
public class KafkaExchange {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchange.class));

  private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private final Map<KafkaTopicExchange, ExchangeState> exchanges;
  private final Map<String, KafkaTopicExchange> addressToExchange;
  private final Map<String, KafkaTopicExchange> topicToExchange;

  public KafkaExchange() {

    this.exchanges = new ConcurrentHashMap<>();
    this.addressToExchange = new ConcurrentHashMap<>();
    this.topicToExchange = new ConcurrentHashMap<>();
  }

  public Optional<KafkaTopicExchange> findByTopic(String topic) {
    rwlock.readLock().lock();
    try {
      return Optional.ofNullable(topicToExchange.get(topic));
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public Optional<KafkaTopicExchange> findByAddress(String address) {
    rwlock.readLock().lock();
    try {
      return Optional.ofNullable(addressToExchange.get(address));
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public Set<String> allExchangeKafkaTopics() {
    rwlock.readLock().lock();
    try {
      return topicToExchange.keySet();
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public Set<String> allReadyToReadKafkaTopics() {
    rwlock.readLock().lock();
    try {
      return exchanges.entrySet().stream()
          .filter(en -> en.getValue().shouldRead())
          .map(en -> en.getKey().kafkaTopicName())
          .collect(Collectors.toSet());
    } finally {
      rwlock.readLock().unlock();
    }

  }

  public boolean exchangeWriteable(KafkaTopicExchange exchange) {
    rwlock.readLock().lock();
    try {
      return Optional.ofNullable(exchanges.get(exchange))
          .map(kte -> !kte.readOnly())
          .orElse(false);
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public boolean exchangeReadable(KafkaTopicExchange exchange) {
    rwlock.readLock().lock();
    try {
      return Optional.ofNullable(exchanges.get(exchange))
          .map(ExchangeState::shouldRead)
          .orElse(false);
    } finally {
      rwlock.readLock().unlock();
    }
  }

  /**
   * Get a copy of the full set of topic exchanges currently known of. The resulting set is
   * immutable.
   */
  public Set<KafkaTopicExchange> getAllExchanges() {
    rwlock.readLock().lock();
    try {
      return ImmutableSet.copyOf(exchanges.keySet());
    } finally {
      rwlock.readLock().unlock();
    }
  }

  private Optional<ExchangeState> withAddressExchange(
      String addressName,
      BiConsumer<KafkaTopicExchange, ExchangeState> mutator) {

    if (this.addressToExchange.containsKey(addressName)) {
      KafkaTopicExchange exchange = this.addressToExchange.get(addressName);
      ExchangeState state = this.exchanges.get(exchange);
      mutator.accept(exchange, state);
      return Optional.of(state);
    }
    return Optional.empty();
  }

  /**
   * Inform the exchange that a binding has been removed from the given AMQ topic address.
   *
   * @param addressName the topic address the binding was removed from
   * @return The new state of the exchange if the address can be mapped to one
   */
  public Optional<ExchangeState> removeReader(String addressName) {
    SLOG.trace(b -> b
        .event("RemoveExchangeReader")
        .putTokens("addressName", addressName));

    rwlock.writeLock().lock();
    try {
      return withAddressExchange(addressName, (exchange, currState) -> {
        ExchangeState newState = this.exchanges.computeIfPresent(
            exchange,
            (k, v) -> v.decrementReaderCount(1));
      });
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  /**
   * Inform the exchange that a binding has been added to the given AMQ topic address.
   *
   * @param addressName the topic address the binding was added to.
   */
  public void addReader(String addressName) {
    SLOG.trace(b -> b
        .event("AddExchangeReader")
        .putTokens("addressName", addressName));

    rwlock.writeLock().lock();
    try {
      withAddressExchange(addressName, (exchange, currState) -> {
        ExchangeState newState = this.exchanges.computeIfPresent(
            exchange,
            (k, v) -> v.incrementReaderCount(1));
      });
    } finally {
      rwlock.writeLock().unlock();
    }

  }

  /**
   * Disables all ingress data for this exchange meaning it should not be published back to Kafka.
   *
   * @param exchange the topic exchange to pause
   */
  public void disableWrite(KafkaTopicExchange exchange) {
    SLOG.info(b -> b
        .event("DisableExchangeWrite")
        .putTokens("exchange", exchange));

    rwlock.writeLock().lock();
    try {
      exchanges.computeIfPresent(exchange, (kte, state) ->
          state.readOnly() ? state : state.disableWrite());
    } finally {
      rwlock.writeLock().unlock();
    }
  }


  /**
   * Allow ingress data into the exchange, meaning data may flow freely across it to Kafka. Enabling
   * write on a writable exchange does nothing.
   *
   * @param exchange the exchange to enable writing on.
   */
  public void enableWrite(KafkaTopicExchange exchange) {
    SLOG.info(b -> b
        .event("ResumeExchange")
        .putTokens("exchange", exchange));

    rwlock.writeLock().lock();
    try {
      exchanges.computeIfPresent(exchange, (kte, state) ->
          state.readOnly() ? state.enableWrite() : state);
    } finally {
      rwlock.writeLock().unlock();
    }

  }

  /**
   * Removes a topic exchange from this exchange meaning it should be disabled and cleaned up if
   * possible.
   *
   * @param exchange the exchange to remove.
   */
  public void removeExchange(KafkaTopicExchange exchange) {
    SLOG.debug(b -> b
        .event("RemoveExchange")
        .putTokens("exchange", exchange));

    rwlock.writeLock().lock();
    try {
      this.addressToExchange.remove(exchange.amqAddressName());
      this.topicToExchange.remove(exchange.kafkaTopicName());
      this.exchanges.remove(exchange);
    } finally {
      rwlock.writeLock().unlock();
    }
  }


  /**
   * Adds a topic exchange to this exchange with the given number of bindings. If the topic exchange
   * was already under management then it will be updated.
   *
   * @param topicExchange the topic exchange to add or update
   * @param writeEnabled  enable the passthrough of ingress data to be written to Kafka
   * @param readerCount   The number of bindings associated to client queues (readers)
   */
  public void addTopicExchange(
      KafkaTopicExchange topicExchange, boolean writeEnabled, int readerCount) {

    rwlock.writeLock().lock();
    try {
      addTopicExchangeWithoutLock(topicExchange, writeEnabled, readerCount);
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  public void addTopicExchanges(
      Map<KafkaTopicExchange, Integer> kteAndReaderMap, boolean writeEnabled) {

    rwlock.writeLock().lock();
    try {
      for (Map.Entry<KafkaTopicExchange, Integer> kteAndReader: kteAndReaderMap.entrySet()) {
        addTopicExchangeWithoutLock(kteAndReader.getKey(), writeEnabled, kteAndReader.getValue());
      }
    } finally {
      rwlock.writeLock().unlock();
    }

  }

  private void addTopicExchangeWithoutLock(
      KafkaTopicExchange topicExchange, boolean writeEnabled, int readerCount) {

    ExchangeState newState = new ExchangeState.Builder()
        .readerCount(readerCount)
        .alwaysAttemptRead(topicExchange.originConfig().consumeAlways())
        .readOnly(!writeEnabled)
        .build();

    this.exchanges.put(topicExchange, newState);
    this.addressToExchange.put(topicExchange.amqAddressName(), topicExchange);
    this.topicToExchange.put(topicExchange.kafkaTopicName(), topicExchange);
  }

  @FreeBuilder
  interface ExchangeState {

    default ExchangeState disableWrite() {
      if (readOnly()) {
        return this;
      } else {
        return new Builder().mergeFrom(this).readOnly(true).build();
      }
    }

    default ExchangeState enableWrite() {
      if (!readOnly()) {
        return this;
      } else {
        return new Builder().mergeFrom(this).readOnly(false).build();
      }
    }

    default ExchangeState decrementReaderCount(int amount) {
      int newCount = Math.max(0, readerCount() - amount);
      return new Builder().mergeFrom(this).readerCount(newCount).build();
    }

    default ExchangeState incrementReaderCount(int amount) {
      return new Builder().mergeFrom(this).readerCount(readerCount() + amount).build();
    }

    /**
     * Whether the corresponding kafka topic is read accessible and their are interested consumers
     * available on the JMS side (bindings).
     *
     * @return
     */
    default boolean shouldRead() {
      return (alwaysAttemptRead() || readerCount() > 0) && canRead();
    }

    /**
     * Whether this exchange can be read or not (consume from Kafka).
     */
    boolean canRead();

    /**
     * Always try to read this exchange (consume from Kafka) regardless of the current number of
     * readers. This will not override {@link #canRead()} if it returns <code>false</code>.
     */
    boolean alwaysAttemptRead();

    /**
     * Can data published to this exchange be published back to Kafka or not. If readOnly is set to
     * true then data will not be published to Kafka.
     */
    boolean readOnly();

    /**
     * The current number of readers attached to this exchange. It should not include internal
     * readers such as the divert.
     */
    int readerCount();

    class Builder extends KafkaExchange_ExchangeState_Builder {

      Builder() {
        canRead(true);
        readOnly(false);
      }

      @Override
      public ExchangeState build() {
        return super.build();
      }
    }
  }
}
