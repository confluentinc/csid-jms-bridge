/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.logging.StructuredLogger;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public abstract class ConsumerThread<K, V> implements Runnable, ConsumerRebalanceListener {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(ConsumerThread.class));

  private static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();

  public static Builder<byte[], byte[]> newByteArrayBuilder(Map<String, Object> consumerProps) {
    return new Builder<byte[], byte[]>()
        .putAllConsumerProps(consumerProps)
        .putConsumerProps(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .keyDeser(BYTE_ARRAY_DESERIALIZER)
        .valueDeser(BYTE_ARRAY_DESERIALIZER);
  }

  public static <K, V> Builder<K, V> newBuilder(Deserializer<K> keyDeser,
      Deserializer<V> valDeser) {
    return new Builder<K, V>()
        .keyDeser(keyDeser)
        .valueDeser(valDeser);
  }

  public static <K, V> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  public abstract UncaughtExceptionHandler exceptionHandler();

  public abstract Map<String, Object> consumerProps();

  public abstract String groupId();

  public abstract Deserializer<K> keyDeser();

  public abstract Deserializer<V> valueDeser();

  public abstract Collection<String> topics();

  public abstract MessageReciever<K, V> receiver();

  public abstract ConsumerCloseListener closeListener();

  public abstract Long pollMs();


  private final AtomicReference<Collection<String>> topicsRef =
      new AtomicReference<>(Collections.emptyList());
  private final AtomicReference<Collection<String>> topicUpdateRef = new AtomicReference<>(null);
  private final Semaphore runFlag = new Semaphore(1);
  private volatile boolean running = false;

  private Long commitInterval() {
    return Long.valueOf(consumerProps()
        .getOrDefault(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
        .toString());
  }

  public void stop(boolean wait) {
    if (running) {
      running = false;

      if (wait) {
        try {
          runFlag.acquire();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          runFlag.release();
        }
      }
    }
  }

  public boolean isRunning() {
    return running;
  }

  public Collection<String> currTopics() {
    return topicsRef.get();
  }

  public void updateTopics(Collection<String> newTopics) {
    topicUpdateRef.set(newTopics);
  }

  @Override
  public void run() {
    try {
      runFlag.acquire();
      running = true;

      try (KafkaConsumer<K, V> consumer = createConsumer()) {
        while (running) {
          if (!consumer.subscription().isEmpty()) {
            ConsumerRecords<K, V>  records = consumer.poll(Duration.ofMillis(pollMs()));
            for (ConsumerRecord<K, V> r: records) {
              receiver().onRecieve(r);
            }

          }

          Collection<String> updatedTopicList = topicUpdateRef.getAndSet(null);
          if (updatedTopicList != null && !updatedTopicList.isEmpty()) {
            SLOG.debug(b -> b.event("UpdateTopicSubscription")
                .name(groupId())
                .putTokens("topicList", updatedTopicList));
            consumer.unsubscribe();
            consumer.subscribe(updatedTopicList, this);
          }
        }

      } finally {
        runFlag.release();
        closeListener().onClose();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaConsumer<K, V> createConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.putAll(consumerProps());
    return new KafkaConsumer<>(props, keyDeser(), valueDeser());
  }

  public static class Builder<K, V> extends ConsumerThread_Builder<K, V> {

    public Builder() {
      pollMs(100L);
      exceptionHandler((thread, err) -> SLOG.error(b -> b
          .event("UncaughtException")
          .name(thread.getName()), err));

      closeListener(() -> {
      });
    }

    @Override
    public ConsumerThread<K, V> build() {
      putConsumerProps(ConsumerConfig.GROUP_ID_CONFIG, groupId());
      return super.build();
    }
  }

  /////
  // ConsumerRebalanceListener IMPL
  ////
  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    Collection<String> currTopics = topicsRef.get();
    if (!currTopics.isEmpty()) {
      Set<String> revocations = partitions.stream()
          .map(TopicPartition::topic)
          .collect(Collectors.toSet());
      List<String> updatedTopics = currTopics.stream()
          .filter(t -> !revocations.contains(t))
          .collect(Collectors.toList());
      topicsRef.set(updatedTopics);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    List<String> currTopics =
        partitions.stream().map(TopicPartition::topic).collect(Collectors.toList());
    topicsRef.set(currTopics);
  }

  public interface MessageReciever<K, V> {

    void onRecieve(ConsumerRecord<K, V> kafkaRecord);
  }

  public interface ConsumerCloseListener {

    void onClose();
  }
}
