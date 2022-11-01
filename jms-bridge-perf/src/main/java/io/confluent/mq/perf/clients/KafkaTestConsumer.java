/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class KafkaTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestConsumer.class);
  private static final LongDeserializer LONG_DESER = new LongDeserializer();
  private static final StringDeserializer STRING_DESER = new StringDeserializer();

  private final String testId;
  private final Properties kafkaProps;
  private final Duration pollDuration;
  private final String topic;
  private final CompletableFuture<Void> initCompleted = new CompletableFuture<>();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).build());
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
      consumer.seekToEnd(Collections.emptyList());
      initCompleted.complete(null);
      while (runCount > msgCount) {
        msgCount += poll(consumer);
      }

      LOGGER.info("Draining remaining data.");
      drain(consumer);

    } catch (Exception e) {
      LOGGER.error("Kafka consumer has encountered problems and is stopping.", e);
    }
    try {
      LOGGER.info("Awaiting for consumer sampling threads to finish");
      executorService.awaitTermination(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      LOGGER.error("Consumer sampling threads did not terminate properly", e);
    }

    LOGGER.info("Kafka consumer finished.");
    return msgCount;
  }

  private long poll(KafkaConsumer<byte[], byte[]> consumer) {
    long msTs = System.currentTimeMillis();
    ConsumerRecords<byte[], byte[]> records = consumer.poll(pollDuration);
    if (records.count() > 0) {
      executorService.execute(() -> recordSample(msTs, records));
    }
    return records.count();
  }

  private long drain(KafkaConsumer<byte[], byte[]> consumer) {
    Map<TopicPartition, Long> endoffsets = findLag(consumer);

    long msgCount = 0L;
    RateLimiter limiter = RateLimiter.create(1, Duration.ofSeconds(30));
    while (!endoffsets.isEmpty()) {
      msgCount += poll(consumer);
      if (limiter.tryAcquire(30)) {
        endoffsets = findLag(consumer);
        LOGGER.info("Current lag: {}",
            endoffsets.values().stream().mapToLong(Long::longValue).sum());
      }
    }
    return msgCount;
  }

  private Map<TopicPartition, Long> findLag(KafkaConsumer<byte[], byte[]> consumer) {
    Map<TopicPartition, Long> endoffsets = consumer.endOffsets(consumer.assignment());
    return consumer.assignment().stream()
        .filter(endoffsets::containsKey)
        .map(tp -> {
          long position = consumer.position(tp);
          return new Pair<>(tp, endoffsets.get(tp) - position);
        })
        .filter(tpLagPair -> tpLagPair.getB() >= 1)
        .collect(Collectors.toMap(Pair::getA, Pair::getB));
  }

  public void stop() {
    this.runCount = 0;
  }

  public void stopAtCount(long stopCount) {
    this.runCount = stopCount;
  }

  protected void recordSample(
      long consumeTs,
      ConsumerRecords<byte[], byte[]> records) {

    CommonMetrics.MSG_CONSUMED_COUNT.inc(records.count());
    for (ConsumerRecord<byte[], byte[]> record : Lists.reverse(Lists.newLinkedList(records))) {
      try {
        Header testIdHdr = record.headers().lastHeader("jms.string.test_id");
        if (testIdHdr != null && testIdHdr.value() != null) {
          String msgTestId = STRING_DESER.deserialize(null, testIdHdr.value());

          if (testId.equals(msgTestId)) {
            if (record.headers().lastHeader("jms.long.test_msg_ms") != null) {
              long produceTs = LONG_DESER.deserialize(
                  null, record.headers().lastHeader("jms.long.test_msg_ms").value());

              LOGGER.info("Calculated latency: ConsumeTs - produceTs = latencyMs, {} - {} = {}",
                  consumeTs, produceTs, consumeTs - produceTs);
              CommonMetrics.MESSAGE_LATENCY_HIST.observe(consumeTs - produceTs);
              CommonMetrics.MSG_CONSUMED_SIZE_GAUGE.set(record.serializedValueSize());
              break;
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Failed to parse message details.", e);
      }
    }
  }
}
