/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.test.TestSupport.println;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@Tag("IntegrationTest")
public class KafkaJournalStoreLoaderTest {

  @RegisterExtension
  @Order(100)
  public static final KafkaTestContainer kafkaContainer = KafkaTestContainer.usingDefaults();

  @TempDir
  Path tempdir;

  Properties defaultStreamProps() {
    Properties streamProps = kafkaContainer.defaultProps();
    streamProps
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "junit-stream-app-" + System.currentTimeMillis());
    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
    streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
    streamProps.put(StreamsConfig.STATE_DIR_CONFIG, tempdir.toAbsolutePath().toString());
    return streamProps;
  }

  @Test
  @Timeout(30)
  public void entireTopicIsLoaded() throws Exception {
    String journalTopic = "junit-journal-1";

    //create journal topic
    kafkaContainer.createTempTopic(journalTopic, 1, ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    ));

    KafkaJournalProcessor processor = null;
    try {

      publishAddRecords(journalTopic, 1, 100);

      processor = new KafkaJournalProcessor(
          journalTopic, defaultStreamProps());
      processor.init();

      LoaderCallbackData loaderData = LoaderCallbackData.build();
      processor.startAndLoad(loaderData.callback);
      println("Waiting for load info");
      JournalLoadInformation jli = loaderData.callback.getLoadInfo();

      assertEquals(100, jli.getNumberOfRecords());
      long id = 1;
      for (RecordInfo ri : loaderData.committedRecords) {
        assertEquals(id, ri.id);
        id++;
      }

    } finally {
      if (processor != null) {
        processor.stop();
      }
    }
  }

  @Test
  @Timeout(30)
  public void loaderOnlyLoadsExactMessageCount() throws Exception {
    String journalTopic = "junit-journal-2";

    //create journal topic
    kafkaContainer.createTempTopic(journalTopic, 1, ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    ));

    KafkaJournalProcessor processor1 = null;
    KafkaJournalProcessor processor2 = null;
    try {

      publishAddRecords(journalTopic, 1, 100);

      processor1 = new KafkaJournalProcessor(
          journalTopic, defaultStreamProps());
      processor1.init();

      LoaderCallbackData loaderData = LoaderCallbackData.build();
      println("Waiting for load info");
      processor1.startAndLoad(loaderData.callback);

      //let stream do some processing
      Thread.sleep(500);
      processor1.stop();
      Thread.sleep(500);

      processor2 = new KafkaJournalProcessor(
          journalTopic, defaultStreamProps());
      processor2.init();
      loaderData = LoaderCallbackData.build();
      println("Waiting for load info #2");
      processor2.startAndLoad(loaderData.callback);
      JournalLoadInformation jli = loaderData.callback.getLoadInfo();

      assertEquals(100, jli.getNumberOfRecords());
      long id = 1;
      for (RecordInfo ri : loaderData.committedRecords) {
        assertEquals(id, ri.id);
        id++;
      }

    } finally {
      if (processor1 != null) {
        processor1.stop();
      }
      if (processor2 != null) {
        processor2.stop();
      }
    }

  }

  @Test
  @Timeout(30)
  public void annotationsAreDeletedWithMessages() throws Exception {
    String journalTopic = "junit-journal-3";

    //create journal topic
    kafkaContainer.createTempTopic(journalTopic, 1, ImmutableMap.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    ));

    KafkaJournalProcessor processor = null;
    TestSupport.publishRecord(kafkaContainer, journalTopic, JournalEntry.newBuilder()
        .setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setProtocolRecordType(ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue())
            .setMessageId(1L)
            .setData(ByteString.copyFrom("payload", StandardCharsets.UTF_8)))
        .build());
    TestSupport.publishRecord(kafkaContainer, journalTopic, JournalEntry.newBuilder()
        .setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setProtocolRecordType(ProtocolRecordType.ADD_REF.getValue())
            .setMessageId(1L)
            .setExtendedId(1)
            .setData(ByteString.copyFrom("queue-ref", StandardCharsets.UTF_8)))
        .build());
    TestSupport.publishRecord(kafkaContainer, journalTopic, JournalEntry.newBuilder()
        .setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setProtocolRecordType(ProtocolRecordType.ACKNOWLEDGE_REF.getValue())
            .setMessageId(1L)
            .setExtendedId(2)
            .setData(ByteString.copyFrom("queue-ref", StandardCharsets.UTF_8)))
        .build());
    TestSupport.publishRecord(kafkaContainer, journalTopic, JournalEntry.newBuilder()
        .setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.DELETE_RECORD)
            .setMessageId(1L))
        .build());

    int finalCount = 0;
    try {
      processor = new KafkaJournalProcessor(
          journalTopic, defaultStreamProps());
      processor.init();

      LoaderCallbackData loaderData = LoaderCallbackData.build();
      println("Waiting for load info");
      processor.startAndLoad(loaderData.callback);

      JournalLoadInformation jli = loaderData.callback.getLoadInfo();
      assertEquals(0, jli.getNumberOfRecords());
      Thread.sleep(2000);
      processor.stop();

      processor = new KafkaJournalProcessor(
          journalTopic, defaultStreamProps());
      processor.init();
      loaderData = LoaderCallbackData.build();
      processor.startAndLoad(loaderData.callback);
      loaderData.callback.getLoadInfo();
      finalCount = loaderData.committedRecords.size();
    } finally {
      processor.stop();
    }

    TestSupport.logJournalFiles(kafkaContainer, journalTopic);
    assertEquals(0, finalCount);
  }

  public void publishAddRecords(String journalTopic, int startIdInclusive, int count)
      throws Exception {

    try (KafkaProducer<JournalEntryKey, JournalEntry> jproducer =
        TestSupport.createJournalProducer(kafkaContainer.defaultProps())) {

      //add 100 records
      for (int i = 0; i < count; i++) {
        JournalEntry v = JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setMessageId(startIdInclusive + i)
            .setProtocolRecordType(ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue())
            .setData(ByteString.copyFrom("Payload", StandardCharsets.UTF_8)))
            .build();
        JournalEntryKey k = KafkaRecordUtils.keyFromEntry(v);
        jproducer.send(new ProducerRecord<>(journalTopic, k, v)).get();
      }
    }
  }

  public static class LoaderCallbackData implements TransactionFailureCallback {

    public static LoaderCallbackData build() {
      return new LoaderCallbackData();
    }

    final List<RecordInfo> committedRecords = new LinkedList<>();
    final List<PreparedTransactionInfo> preppedTxs = new LinkedList<>();
    final Map<Long, TxFailedRecords> failedTxids = new HashMap<>();
    final KafkaJournalLoaderCallback callback;

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
}
