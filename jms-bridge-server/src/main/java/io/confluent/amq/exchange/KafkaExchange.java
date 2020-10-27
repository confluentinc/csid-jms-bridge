/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import com.google.common.collect.ImmutableSet;
import io.confluent.amq.logging.StructuredLogger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Contains information on the configuration of how data is exchanged between kakfa topics and amq
 * destinations.
 */
public class KafkaExchange {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchange.class));

  private final Map<KafkaTopicExchange, ExchangeState> exchanges;
  private final Map<String, KafkaTopicExchange> addressToExchange;
  private final List<ExchangeChangeListener> listeners;

  public KafkaExchange() {

    this.exchanges = new ConcurrentHashMap<>();
    this.addressToExchange = new ConcurrentHashMap<>();
    this.listeners = new LinkedList<>();
  }

  public boolean exchangeWriteable(KafkaTopicExchange exchange) {
    return exchanges.containsKey(exchange)
        && !exchanges.get(exchange).readOnly();
  }

  public boolean exchangeReadable(KafkaTopicExchange exchange) {
    return exchanges.containsKey(exchange) && exchanges.get(exchange).shouldRead();
  }

  /**
   * Get a copy of the full set of topic exchanges currently known of. The resulting set is
   * immutable.
   */
  public Set<KafkaTopicExchange> getAllExchanges() {
    return ImmutableSet.copyOf(exchanges.keySet());
  }

  private void withAddressExchange(
      String addressName,
      BiConsumer<KafkaTopicExchange, ExchangeState> mutator) {

    if (this.addressToExchange.containsKey(addressName)) {
      KafkaTopicExchange exchange = this.addressToExchange.get(addressName);
      ExchangeState state = this.exchanges.get(exchange);
      mutator.accept(exchange, state);
    }
  }

  /**
   * Inform the exchange that a binding has been removed from the given AMQ topic address.
   *
   * @param addressName the topic address the binding was removed from
   */
  public void removeReader(String addressName) {
    SLOG.trace(b -> b
        .event("RemoveExchangeReader")
        .putTokens("addressName", addressName));

    withAddressExchange(addressName, (exchange, currState) -> {
      ExchangeState newState = this.exchanges.computeIfPresent(
          exchange,
          (k, v) -> v.decrementReaderCount(1));

      if (newState != null && currState.shouldRead() && !newState.shouldRead()) {
        listeners.forEach(l -> l.onDisableExchange(exchange));
      }
    });
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

    withAddressExchange(addressName, (exchange, currState) -> {
      ExchangeState newState = this.exchanges.computeIfPresent(
          exchange,
          (k, v) -> v.incrementReaderCount(1));

      if (newState != null && newState.shouldRead() && !currState.shouldRead()) {
        listeners.forEach(l -> l.onEnableExchange(exchange));
      }
    });

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

    if (exchanges.containsKey(exchange) && !exchanges.get(exchange).readOnly()) {
      ExchangeState newState = exchanges
          .computeIfPresent(exchange, (kte, state) -> state.disableWrite());

      if (newState != null && !newState.shouldRead()) {
        listeners.forEach(l -> l.onDisableExchange(exchange));
      }
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

    if (exchanges.containsKey(exchange) && exchanges.get(exchange).readOnly()) {
      ExchangeState newState = exchanges.compute(exchange, (kte, state) ->
          state != null
              ? state.enableWrite()
              : new ExchangeState.Builder().readOnly(false).readerCount(0).build());

      if (newState.shouldRead()) {
        listeners.forEach(l -> l.onEnableExchange(exchange));
      }
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

    this.addressToExchange.remove(exchange.amqAddressName());
    this.exchanges.remove(exchange);

    listeners.forEach(l -> l.onRemoveExchange(exchange));
  }


  /**
   * Adds a topic exchange to this exchange with the given number of bindings. If the topic exchange
   * was already under management then it will be updated.
   *
   * @param topicExchange the topic exchange to add or update
   * @param writeEnabled  enable the passthrough of ingress data to be written to Kafka
   * @param readerCount   The number of bindings associated to client queues (readers)
   */
  public synchronized void addTopicExchange(
      KafkaTopicExchange topicExchange, boolean writeEnabled, int readerCount) {

    ExchangeState newState = new ExchangeState.Builder()
        .readerCount(readerCount)
        .alwaysAttemptRead(topicExchange.originConfig().consumeAlways())
        .readOnly(!writeEnabled)
        .build();

    ExchangeState oldState = this.exchanges.put(topicExchange, newState);
    this.addressToExchange.put(topicExchange.amqAddressName(), topicExchange);

    if (oldState != null) {
      if (oldState.readOnly() && !newState.readOnly()) {
        listeners.forEach(l -> l.onEnableExchange(topicExchange));
      } else if (!oldState.readOnly() && newState.readOnly()) {
        listeners.forEach(l -> l.onDisableExchange(topicExchange));
      }
    } else {
      listeners.forEach(l -> l.onAddExchange(topicExchange));
    }
  }

  /**
   * This is the central means in staying up to date with what topic exchanges are currently being
   * managed and their enabled state. Any time a topic exchange is removed/added/enabled/disabled
   * for any reason all listeners will be invoked. Be aware that any processing happening within a
   * listener's method will be done on the exchanges thread.
   *
   * @param listener a listener that will be invoked on state changes.
   */
  public void registerListener(ExchangeChangeListener listener) {
    this.listeners.add(listener);
  }

  public interface ExchangeChangeListener {

    /**
     * Triggered whenever a topic exchange is added to this exchange.
     *
     * @param topicExchange the added exchange.
     */
    default void onAddExchange(KafkaTopicExchange topicExchange) {

    }

    /**
     * Triggered whenever a topic exchange is removed from this exchange.
     *
     * @param topicExchange the removed topic exchange
     */
    default void onRemoveExchange(KafkaTopicExchange topicExchange) {

    }

    /**
     * Triggered whenever a topic exchange is disabled. Disabled exchanges should not flow data.
     *
     * @param topicExchange the disabled topic exchange
     */
    default void onDisableExchange(KafkaTopicExchange topicExchange) {

    }

    /**
     * Triggered whenever a topic exchange is enabled. Enabled exchanges may flow data.
     *
     * @param topicExchange the disabled topic exchange
     */
    default void onEnableExchange(KafkaTopicExchange topicExchange) {

    }
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
