/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import io.confluent.amq.persistence.kafka.journal.serde.ProtoSerializer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestSupport {

  /**
   * Used for test logging
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(TestSupport.class);

  private TestSupport() {
  }

  public static void println(String format, Object... objects) {
    LOGGER.info(format, objects);
  }


  public static Stream<Pair<JournalEntryKey, JournalEntry>> streamJournalFiles(
      KafkaTestContainer kafkaContainer, String journalTopic) {

    return kafkaContainer
        .consumeAll(journalTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        .stream()
        .map(r -> {

          JournalEntryKey rkey = null;
          if (r.key() != null) {
            try {
              rkey = JournalEntryKey.parseFrom(r.key());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          JournalEntry rval = null;
          if (r.value() != null) {
            try {
              rval = JournalEntry.parseFrom(r.value());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return Pair.of(rkey, rval);
        });
  }

  public static void logJournalFiles(KafkaTestContainer kafkaContainer, String journalTopic) {
    logJournalFiles(kafkaContainer, journalTopic, false);
  }

  public static void logJournalFiles(
      KafkaTestContainer kafkaContainer, String journalTopic, boolean doCompact) {

    Stream<Pair<JournalEntryKey, JournalEntry>> journalStream;

    if (doCompact) {
      journalStream = getCompactedJournal(kafkaContainer, journalTopic)
          .entrySet()
          .stream()
          .map(en -> Pair.of(en.getKey(), en.getValue()));

    } else {
      journalStream = streamJournalFiles(kafkaContainer, journalTopic);

    }

    String title = String.format(
        "#### JOURNAL%s FOR TOPIC %s ####",
        doCompact ? "(COMPACTED)" : "",
        journalTopic);

    logJournal(title, journalStream);
  }

  public static void logTable(String journalName, Map<JournalEntryKey, JournalEntry> table) {
    String title = String.format("#### JOURNAL %s TABLE ####", journalName);

    logJournal(title, table != null
        ? table.entrySet().stream().map(en -> Pair.of(en.getKey(), en.getValue()))
        : Stream.empty());

  }

  private static void logJournal(
      String title, Stream<Pair<JournalEntryKey, JournalEntry>> stream) {

    LogFormat format = LogFormat.forSubject("JournalLog");
    String journalStr = stream
        .map(pair -> format.build(b -> {

          b.addJournalEntryKey(pair.getKey());
          if (pair.getValue() == null) {
            b.event("TOMBSTONE");
          } else {
            b.event("ENTRY");
            b.addJournalEntry(pair.getValue());
          }

        }))
        .collect(Collectors.joining(System.lineSeparator()));

    println(System.lineSeparator() + title + System.lineSeparator() + journalStr);

  }

  /**
   * The iteration order of this mirrors that of the compacted log.
   */
  public static Map<JournalEntryKey, JournalEntry> getCompactedJournal(
      KafkaTestContainer kafkaContainer, String journalTopic) {

    Map<JournalEntryKey, JournalEntry> table =
        new LinkedHashMap<>(100, 0.75f, true);

    streamJournalFiles(kafkaContainer, journalTopic).forEachOrdered(
        kv -> {
          if (kv.getValue() == null) {
            table.remove(kv.getKey());
          } else {
            table.put(kv.getKey(), kv.getValue());
          }
        });

    return table;
  }

  public static KafkaProducer<JournalEntryKey, JournalEntry> createJournalProducer(
      Properties baseConfig) {

    Properties producerProps = new Properties();
    producerProps.putAll(baseConfig);
    producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
        JournalEntryKeyPartitioner.class.getCanonicalName());

    return new KafkaProducer<>(producerProps, new ProtoSerializer<>(), new ProtoSerializer<>());
  }

  /**
   * Generate several ADD records.
   */
  public static void publishAddRecords(
      KafkaTestContainer kafkaContainer,
      String journalTopic,
      int startIdInclusive,
      int count) throws Exception {

    publishRecords(
        kafkaContainer, journalTopic, JournalRecordType.ADD_RECORD, startIdInclusive, count);
  }

  public static void publishRecord(
      KafkaTestContainer kafkaContainer,
      String journalTopic,
      JournalEntry journalEntry) throws Exception {

    try (KafkaProducer<JournalEntryKey, JournalEntry> jproducer =
        TestSupport.createJournalProducer(kafkaContainer.defaultProps())) {

      jproducer.send(new ProducerRecord<>(journalTopic,
          KafkaRecordUtils.keyFromEntry(journalEntry), journalEntry)).get();
    }

  }

  public static void publishRecords(
      KafkaTestContainer kafkaContainer,
      String journalTopic,
      JournalRecordType recordType,
      int startingMessageId,
      int count) throws Exception {

    try (KafkaProducer<JournalEntryKey, JournalEntry> jproducer =
        TestSupport.createJournalProducer(kafkaContainer.defaultProps())) {

      //add 100 records
      for (int i = 0; i < count; i++) {
        JournalEntry v = JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(recordType)
            .setMessageId(startingMessageId + i)
            .setProtocolRecordType(ProtocolRecordType.UNASSIGNED.getValue())
            .setData(ByteString.copyFrom("Payload", StandardCharsets.UTF_8)))
            .build();
        JournalEntryKey k = KafkaRecordUtils.keyFromEntry(v);
        jproducer.send(new ProducerRecord<>(journalTopic, k, v)).get();
      }
    }
  }

  public static TopologyTestDriver createStreamsTestDriver(
      Topology topology, Properties streamProps) {

    Properties props = new Properties();
    props.putAll(streamProps);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    return new TopologyTestDriver(topology, props);
  }

  public static void retry(int attempts, int delayMs, RunnableWithScissors test) {
    int tryCount = 0;
    Throwable fail = null;
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (tryCount < attempts) {
      try {
        tryCount++;
        test.run();
        fail = null;
        break;
      } catch (Throwable t) {
        fail = t;
        if (delayMs > 0) {
          try {
            Thread.sleep(delayMs);
          } catch (Exception e) {
            fail = e;
          }
        }
      }
    }
    stopwatch.stop();
    if (fail != null) {
      throw new AssertionFailedError(
          String.format(
              "Failed after %d attempts over %d ms",
              tryCount,
              stopwatch.elapsed(TimeUnit.MILLISECONDS)), fail);
    }
  }

  public interface RunnableWithScissors {
    void run() throws Exception;
  }
}

