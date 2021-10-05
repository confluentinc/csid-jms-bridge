/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestConsumer.class);
  private static final LongDeserializer LONG_DESER = new LongDeserializer();
  private static final StringDeserializer STRING_DESER = new StringDeserializer();

  private final String testId;
  private final Properties kafkaProps;
  private final Duration pollDuration;
  private final String topic;
  private final CompletableFuture<Void> initCompleted = new CompletableFuture<>();
  private volatile long runCount = Long.MAX_VALUE;

  public KafkaTestConsumer(
      Properties kafkaProps,
      String topic,
      Duration pollDuration,
      String testId) {

    this.testId = testId;
    this.kafkaProps = kafkaProps;
    this.pollDuration = pollDuration;
    this.topic = topic;
  }

  public CompletableFuture<Void> getInitCompleted() {
    return initCompleted;
  }

  public Long start() {
    long msgCount = 0;
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
        kafkaProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {

      consumer.subscribe(Arrays.asList(topic));
      consumer.seekToEnd(consumer.assignment());
      initCompleted.complete(null);
      while (runCount > msgCount) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
        long msTs = System.currentTimeMillis();
        if (records.count() > 0) {
          msgCount += recordSample(msTs, records);
        }
      }

    } catch (Exception e) {
      LOGGER.error("Kafka consumer has encountered problems and is stopping.", e);
    }
    LOGGER.info("Kafka consumer finished.");
    return msgCount;
  }

  public void stopAtCount(long stopCount) {
    this.runCount = stopCount;
  }

  protected int recordSample(
      long consumeTs,
      ConsumerRecords<byte[], byte[]> records) {

    CommonMetrics.MSG_CONSUMED_COUNT.inc(records.count());
    ConsumerRecord<byte[], byte[]> lastRecord = Iterables.getLast(records);
    try {
      String msgTestId = lastRecord.headers().lastHeader("jms.test_id") != null
          ? STRING_DESER.deserialize(null,
          lastRecord.headers().lastHeader("jms.test_id").value())
          : "NAH";

      if (testId.equals(msgTestId)) {
        if (lastRecord.headers().lastHeader("jms.test_msg_ms") != null) {
          long produceTs = LONG_DESER.deserialize(
              null, lastRecord.headers().lastHeader("jms.test_msg_ms").value());

          CommonMetrics.MESSAGE_LATENCY_HIST.observe(consumeTs - produceTs);
          CommonMetrics.MSG_CONSUMED_SIZE_GAUGE.set(lastRecord.serializedValueSize());
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to parse message details.", e);
    }

    return records.count();
  }
}
