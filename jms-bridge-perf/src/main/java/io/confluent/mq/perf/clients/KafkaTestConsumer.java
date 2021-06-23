/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.mq.perf.TestTime;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class KafkaTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestConsumer.class);
  private static final LongDeserializer LONG_DESER = new LongDeserializer();
  private static double[] BUCKETS = {
      1, 5, 10, 25, 50, 100, 250, 500, 750, 1000, 1500, 2000, 3000, 4000, 5000
  };

  private static final Gauge MSG_SIZE_GAUGE = Gauge.build()
      .name("perf_test_message_size")
      .help("The size of the message being received")
      .register();

  private static final Histogram P2C_HIST = Histogram.build()
      .buckets(BUCKETS)
      .name("perf_test_producer_to_consumer_latency_ms")
      .help("Time it takes for message to be consumed from the producer")
      .register();

  private final TestTime ttime;
  private final Properties kafkaProps;
  private final Duration pollDuration;
  private final String topic;
  private final Executor executor = Executors.newSingleThreadExecutor();
  private final ClientThroughputSync sync;

  public KafkaTestConsumer(
      Properties kafkaProps,
      String topic,
      Duration pollDuration,
      TestTime ttime,
      ClientThroughputSync sync) {

    this.ttime = ttime;
    this.kafkaProps = kafkaProps;
    this.pollDuration = pollDuration;
    this.topic = topic;
    this.sync = sync;
  }

  public void start() {
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
        kafkaProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {

      List<TopicPartition> assignment = consumer.partitionsFor(topic).stream()
          .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
          .collect(Collectors.toList());
      consumer.assign(assignment);
      consumer.seekToEnd(consumer.assignment());

      boolean dataRemains = true;
      sync.signalReady();
      while (dataRemains) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
        long msTs = System.currentTimeMillis();
        if (records.count() > 0) {
          executor.execute(() -> recordSample(msTs, records));
        }

        dataRemains = sync.awaitNext(records.count() + 1, Duration.ofSeconds(30));
      }

    } catch (Exception e) {
      LOGGER.error("Kafka consumer has encountered problems and is stopping.", e);
    }

    executor.execute(sync::signalComplete);
    LOGGER.info("Kafka consumer finished.");
  }

  protected void recordSample(
      long consumeTs,
      ConsumerRecords<byte[], byte[]> records) {

    for (ConsumerRecord<byte[], byte[]> record : records) {
      try {
        long testStartMs = LONG_DESER.deserialize(
            null, record.headers().lastHeader("jms.test_start_ms").value());

        if (testStartMs == ttime.startTime()) {
          long produceTs = LONG_DESER.deserialize(
              null, record.headers().lastHeader("jms.test_msg_ms").value());

          P2C_HIST.observe(consumeTs - produceTs);

          MSG_SIZE_GAUGE.set(record.serializedValueSize());
        }

      } catch (Exception e) {
        LOGGER.error("Failed to parse message details.", e);
      }
    }
  }
}
