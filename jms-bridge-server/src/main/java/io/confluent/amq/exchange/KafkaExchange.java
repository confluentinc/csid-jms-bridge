/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import com.google.common.collect.ImmutableSet;
import io.confluent.amq.logging.StructuredLogger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
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
  private AtomicBoolean paused = new AtomicBoolean(true);

  public KafkaExchange() {

    this.exchanges = new ConcurrentHashMap<>();
    this.addressToExchange = new ConcurrentHashMap<>();
    this.listeners = new LinkedList<>();
  }

  public boolean isExchangePaused(KafkaTopicExchange exchange) {
    return paused.get()
        || !exchanges.containsKey(exchange)
        || exchanges.get(exchange).paused();
  }

  public boolean isExchangeEnabled(KafkaTopicExchange exchange) {
    if (paused.get()) {
      return false;
    }

    return exchanges.containsKey(exchange) && exchanges.get(exchange).isEnabled();
  }

  /**
   * Get a copy of the full set of topic exchanges currently known of. The resulting set is
   * immutable.
   */
  public Set<KafkaTopicExchange> getAllExchanges() {
    return ImmutableSet.copyOf(exchanges.keySet());
  }

  /**
   * Get a copy of the full set of enabled topic exchanges currently known of. The resulting set is
   * immutable. See {@link #isExchangeEnabled(KafkaTopicExchange)}.
   */
  public Set<KafkaTopicExchange> getEnabledExchanges() {
    if (paused.get()) {
      return Collections.emptySet();
    }

    return exchanges.entrySet()
        .stream()
        .filter(en -> en.getValue().isEnabled())
        .map(Entry::getKey)
        .collect(Collectors.toSet());
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
  public void bindingRemoved(String addressName) {
    SLOG.trace(b -> b
        .event("RemoveBinding")
        .putTokens("addressName", addressName));

    withAddressExchange(addressName, (exchange, currState) -> {
      ExchangeState newState = this.exchanges.computeIfPresent(
          exchange,
          (k, v) -> v.decreaseBindingCount(1));

      if (newState != null && currState.isEnabled() && !newState.isEnabled()) {
        listeners.forEach(l -> l.onDisableExchange(exchange));
      }
    });
  }

  /**
   * Inform the exchange that a binding has been added to the given AMQ topic address.
   *
   * @param addressName the topic address the binding was added to.
   */
  public void bindingAdded(String addressName) {
    SLOG.trace(b -> b
        .event("AddBinding")
        .putTokens("addressName", addressName));

    withAddressExchange(addressName, (exchange, currState) -> {
      ExchangeState newState = this.exchanges.computeIfPresent(
          exchange,
          (k, v) -> v.incrementBindingCount(1));

      if (newState != null && newState.isEnabled() && !currState.isEnabled()) {
        listeners.forEach(l -> l.onEnableExchange(exchange));
      }
    });

  }

  /**
   * Pauses an exchange meaning data should not move through it at all.
   *
   * @param exchange the topic exchange to pause
   */
  public void pauseExchange(KafkaTopicExchange exchange) {
    SLOG.info(b -> b
        .event("PauseExchange")
        .putTokens("exchange", exchange));

    if (exchanges.containsKey(exchange) && !exchanges.get(exchange).paused()) {
      ExchangeState newState = exchanges.computeIfPresent(exchange, (kte, state) -> state.pause());

      if (newState != null && !newState.isEnabled()) {
        listeners.forEach(l -> l.onDisableExchange(exchange));
      }
    }
  }


  /**
   * Resume the exchange if it is paused, meaning data may flow freely across it once again.
   *
   * @param exchange the exchange to unpause or resume.
   */
  public void resumeExchange(KafkaTopicExchange exchange) {
    SLOG.info(b -> b
        .event("ResumeExchange")
        .putTokens("exchange", exchange));

    if (exchanges.containsKey(exchange) && exchanges.get(exchange).paused()) {
      ExchangeState newState = exchanges.compute(exchange, (kte, state) ->
          state != null
              ? state.resume()
              : new ExchangeState.Builder().paused(false).bindingCount(0).build());

      if (newState.isEnabled()) {
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
   * @param bindingCount  the binding count to associate to the exchange
   */
  public synchronized void addTopicExchange(
      KafkaTopicExchange topicExchange, int bindingCount) {

    ExchangeState newState = new ExchangeState.Builder()
        .bindingCount(bindingCount)
        .ignoreBindingCount(topicExchange.originConfig().consumeAlways())
        .build();

    ExchangeState oldState = this.exchanges.put(topicExchange, newState);
    this.addressToExchange.put(topicExchange.amqAddressName(), topicExchange);

    if (oldState != null) {
      if (oldState.paused() && !newState.paused()) {
        listeners.forEach(l -> l.onEnableExchange(topicExchange));
      } else if (!oldState.paused() && newState.paused()) {
        listeners.forEach(l -> l.onDisableExchange(topicExchange));
      }
    } else {
      listeners.forEach(l -> l.onAddExchange(topicExchange));
    }
  }

  /**
   * Adds a topic exchange to this exchange with the given number of bindings. If the topic exchange
   * was already under management then it will be updated. This assumes a binding count of 0.
   *
   * @param topicExchange the topic exchange to add or update
   */
  public void addTopicExchange(KafkaTopicExchange topicExchange) {
    addTopicExchange(topicExchange, 0);
  }

  /**
   * Pause this entire exchange meaning no data should flow in or out of any of the topic exchanges
   * being managed.
   */
  public void pauseAll() {
    paused.set(true);
  }

  /**
   * Enable this exchange meaning data may flow in or out of any of the managed exchanges if they
   * themselves are not paused or disabled.
   */
  public void enableAll() {
    paused.set(false);
  }

  /**
   * Whether this exchange is currently paused or not.
   *
   * @return true if it is paused
   */
  public boolean isPaused() {
    return paused.get();
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

    default ExchangeState pause() {
      if (paused()) {
        return this;
      } else {
        return new Builder().mergeFrom(this).paused(true).build();
      }
    }

    default ExchangeState resume() {
      if (!paused()) {
        return this;
      } else {
        return new Builder().mergeFrom(this).paused(false).build();
      }
    }

    default ExchangeState updateBindingCount(int newCount) {
      return new Builder().mergeFrom(this).bindingCount(newCount).build();
    }

    default ExchangeState decreaseBindingCount(int amount) {
      int newCount = Math.max(0, bindingCount() - amount);
      return new Builder().mergeFrom(this).bindingCount(newCount).build();
    }

    default ExchangeState incrementBindingCount(int amount) {
      return new Builder().mergeFrom(this).bindingCount(bindingCount() + amount).build();
    }

    default boolean isEnabled() {
      return (ignoreBindingCount() || bindingCount() > 1) && !paused();
    }

    boolean ignoreBindingCount();

    boolean paused();

    int bindingCount();

    class Builder extends KafkaExchange_ExchangeState_Builder {

      Builder() {
        paused(false);
      }

      @Override
      public ExchangeState build() {
        return super.build();
      }
    }
  }
}
