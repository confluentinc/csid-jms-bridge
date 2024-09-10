/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public class EpochCoordinator implements
    KafkaClientSupplier, ConsumerPartitionAssignor, Configurable {

  public static final String EPOCH_STAGE_START = "START";
  public static final String EPOCH_STAGE_READY = "READY";

  public static int initialEpochId() {
    return INITIAL_EPOCH_ID.get();
  }

  public static int currentEpochId() {
    return EPOCH_ID.get();
  }

  private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
      .loggerClass(EpochCoordinator.class));

  private static final String DELEGATE_CONFIG_KEY = "EpochCoordinator.assigner.delegate";
  private static final AtomicInteger INITIAL_EPOCH_ID = new AtomicInteger(-1);
  private static final AtomicInteger EPOCH_ID = new AtomicInteger(-1);

  //Flag that indicates that epoch messages are injected - new epoch started.
  //Used by GlobalStateProcessor to determine that there are no unprocessed / pending messages for Journal Loading phase.
  private static final AtomicBoolean IS_EPOCH_STARTED = new AtomicBoolean(false);
  private static final KafkaClientSupplier SUPPLIER_INSTANCE = new DefaultKafkaClientSupplier();

  private ConsumerPartitionAssignor assignorDelegate;

  @Override
  public Admin getAdmin(
      Map<String, Object> config) {
    return SUPPLIER_INSTANCE.getAdmin(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(
      Map<String, Object> config) {
    return SUPPLIER_INSTANCE.getProducer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(
      Map<String, Object> config) {
    return SUPPLIER_INSTANCE.getRestoreConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(
      Map<String, Object> config) {
    return SUPPLIER_INSTANCE.getGlobalConsumer(config);
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
    Map<String, Object> newConfigs = new HashMap<>(config);
    newConfigs.put(DELEGATE_CONFIG_KEY, config.get(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
    newConfigs
        .put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, EpochCoordinator.class.getName());
    return SUPPLIER_INSTANCE.getConsumer(newConfigs);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      String delegateClass = configs.get(DELEGATE_CONFIG_KEY).toString();
      assignorDelegate =
          (ConsumerPartitionAssignor) Class.forName(delegateClass)
              .getDeclaredConstructor().newInstance();
      if (assignorDelegate instanceof Configurable) {
        ((Configurable) assignorDelegate).configure(configs);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
    return assignorDelegate.assign(metadata, groupSubscription);
  }

  @Override
  public String name() {
    return assignorDelegate.name();
  }

  @Override
  public ByteBuffer subscriptionUserData(Set<String> topics) {
    return assignorDelegate.subscriptionUserData(topics);
  }

  @Override
  public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
    if (assignorDelegate != null) {
      assignorDelegate.onAssignment(assignment, metadata);
    }

    int previousId = EPOCH_ID.getAndSet(metadata.generationId());
      SLOG.debug(b -> b.name("EpochCoordinator")
                      .event("onAssignment")
                      .putTokens("generationId", metadata.generationId())
                      .putTokens("previousId", previousId));
    if (previousId == -1) {
        INITIAL_EPOCH_ID.compareAndSet(-1, metadata.generationId());
        SLOG.debug(b -> b.name("EpochCoordinator")
                        .event("EpochStartFound"));
      IS_EPOCH_STARTED.set(true);
    }
  }

  @Override
  public List<RebalanceProtocol> supportedProtocols() {
    return assignorDelegate.supportedProtocols();
  }

  @Override
  public short version() {
    return assignorDelegate.version();
  }

  public int getInitialEpoch() {
    return initialEpochId();
  }

  public boolean isEpochStarted() {
    return IS_EPOCH_STARTED.get();
  }
}

