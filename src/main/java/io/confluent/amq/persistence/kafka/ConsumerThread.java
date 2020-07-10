/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public abstract class ConsumerThread<K, V> implements Runnable {

  private static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();

  public static Builder<byte[], byte[]> newByteArrayBuilder(Map<String, Object> consumerProps) {
    return new Builder<byte[], byte[]>()
        .putAllConsumerProps(consumerProps)
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

  public abstract Map<String, Object> consumerProps();

  public abstract String groupId();

  public abstract Deserializer<K> keyDeser();

  public abstract Deserializer<V> valueDeser();

  public abstract Collection<String> topics();

  public abstract MessageReciever<K, V> receiver();

  public abstract ConsumerCloseListener closeListener();

  public abstract Long pollMs();

  private volatile boolean running = false;

  public void stop() {
    running = false;
  }

  public boolean isRunning() {
    return running;
  }

  @Override
  public void run() {
    running = true;
    try (KafkaConsumer<K, V> consumer = createConsumer()) {
      consumer.subscribe(topics());

      while (running) {
        consumer.poll(Duration.ofMillis(pollMs())).forEach(receiver()::onRecieve);
      }
    } finally {
      running = false;
      closeListener().onClose();
    }
  }

  private KafkaConsumer<K, V> createConsumer() {
    return new KafkaConsumer<>(consumerProps(), keyDeser(), valueDeser());
  }

  public static class Builder<K, V> extends ConsumerThread_Builder<K, V> {

    public Builder() {
      pollMs(100L);

      closeListener(() -> {
      });
    }
  }

  public interface MessageReciever<K, V> {

    void onRecieve(ConsumerRecord<K, V> kafkaRecord);
  }

  public interface ConsumerCloseListener {

    void onClose();
  }
}
