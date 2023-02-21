/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import com.google.common.base.Stopwatch;
import io.confluent.amq.cli.SendCommandTest;
import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalLoaderCallback;
import io.confluent.amq.persistence.kafka.journal.serde.ProtoSerializer;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public final class TestSupport {
  private static final StringDeserializer stringDeser = new StringDeserializer();
  private static final StringSerializer stringSer = new StringSerializer();
  private static final LongDeserializer longDeser = new LongDeserializer();

  public static StringDeserializer stringDeserializer() {
    return stringDeser;
  }

  public static StringSerializer stringSerializer() {
    return stringSer;
  }

  public static LongDeserializer longDeserializer() {
    return longDeser;
  }

  /**
   * Used for test logging
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(TestSupport.class);

  private TestSupport() {
  }

  public static void println(String format, Object... objects) {
    LOGGER.info(format, objects);
  }

  public static Map<String, String> baseStreamProps(Path tempdir) {
    return baseStreamProps(tempdir, Collections.emptyMap());
  }

  public static Map<String, String> baseStreamProps(Path tempdir, Map<String, String> moreProps) {
    Path stateDir = tempdir.resolve("_stream-" + System.currentTimeMillis());

    try {
      Files.createDirectory(stateDir);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Map<String, String> streamProps = new HashMap<>();
    streamProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());
    streamProps.putAll(moreProps);
    return streamProps;
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

  public static KafkaProducer<JournalEntryKey, JournalEntry> createJournalProducer(
      Properties baseConfig) {

    Properties producerProps = new Properties();
    producerProps.putAll(baseConfig);
    producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
        JournalEntryKeyPartitioner.class.getCanonicalName());

    return new KafkaProducer<>(producerProps, new ProtoSerializer<>(), new ProtoSerializer<>());
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

  public static LoaderCallbackData createLoaderCallback() {
    return new LoaderCallbackData();
  }

  public static class LoaderCallbackData implements TransactionFailureCallback {

    public static LoaderCallbackData build() {
      return new LoaderCallbackData();
    }

    public final List<RecordInfo> committedRecords = new LinkedList<>();
    public final List<PreparedTransactionInfo> preppedTxs = new LinkedList<>();
    public final Map<Long, TxFailedRecords> failedTxids = new HashMap<>();
    public final KafkaJournalLoaderCallback callback;

    public LoaderCallbackData() {
      this.callback = KafkaJournalLoaderCallback.from(committedRecords, preppedTxs, this, false);
    }

    @Override
    public void failedTransaction(long transactionID, List<RecordInfo> records,
        List<RecordInfo> recordsToDelete) {
      failedTxids.put(transactionID, new TxFailedRecords(records, recordsToDelete));
    }
  }

  public static class TxFailedRecords {

    final List<RecordInfo> records;
    final List<RecordInfo> deleteRecords;

    public TxFailedRecords(List<RecordInfo> records,
        List<RecordInfo> deleteRecords) {
      this.records = records;
      this.deleteRecords = deleteRecords;
    }
  }

  public static Runnable runner(RunnableWithScissors runner) {
    return () -> {
      try {
        runner.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  public interface RunnableWithScissors {
    void run() throws Exception;
  }

}

