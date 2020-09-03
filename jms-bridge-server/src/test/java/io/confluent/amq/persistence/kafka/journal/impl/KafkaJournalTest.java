/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.test.TestSupport.createStreamsTestDriver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.ByteString;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor.JournalSpec;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KafkaJournalTest {

  @TempDir
  Path tempdir;

  Properties defaultStreamProps() {
    Properties streamProps = new Properties();
    streamProps.put(StreamsConfig.STATE_DIR_CONFIG, tempdir.toAbsolutePath().toString());
    return streamProps;
  }

  @Test
  public void addRecordsDoNotFlowThroughStoreIsCurrent() throws Exception {
    TestHelper test = new TestHelper(defaultStreamProps());

    createAddRecords(0, 100)
        .forEach(kv -> test.inputTopic.pipeInput(kv.key, kv.value));

    assertEquals(0, test.outputTopic.getQueueSize(), "Add records should not flow through!");
    test.withJournalStore(store -> assertEquals(100, store.approximateNumEntries()));
  }

  @Test
  public void deleteCreatesTombstoneUpdatesStore() throws Exception {
    try (TestHelper test = new TestHelper(defaultStreamProps())) {

      withAddRecord(0, kv -> test.inputTopic.pipeInput(kv.key, kv.value));
      withDeleteRecord(0, kv -> test.inputTopic.pipeInput(kv.key, kv.value));

      assertEquals(2, test.outputTopic.getQueueSize(), "No tombstones present.");

      withKeyValue(test.outputTopic.readKeyValue(), (k, v) -> {
        assertEquals(0, k.getMessageId());
        assertEquals(0, k.getExtendedId());
        assertNull(v);
      });

      withKeyValue(test.outputTopic.readKeyValue(), (k, v) -> {
        assertEquals(0, k.getMessageId());
        assertEquals(KafkaRecordUtils.MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT, k.getExtendedId());
        assertNull(v);
      });

      test.withJournalStore(store ->
          assertNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(0))));
    }
  }

  @Test
  public void annotationRefsAggregate() throws Exception {
    try (TestHelper test = new TestHelper(defaultStreamProps())) {

      withAddRecord(1, kv -> test.inputTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey1 = withAnnotateRecord(1, "Ann-1", kv ->
          test.inputTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey2 = withAnnotateRecord(1, "Ann-2", kv ->
          test.inputTopic.pipeInput(kv.key, kv.value));

      assertEquals(2, test.outputTopic.getQueueSize(),
          "Expected 2 annotation reference updates");

      withKeyValue(test.outputTopic.readKeyValue(), (k, v) -> {
        assertEquals(1, k.getMessageId());
        assertEquals(KafkaRecordUtils.MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT, k.getExtendedId());
        assertEquals(1, v.getAnnotationReference().getEntryReferencesCount());
      });

      withKeyValue(test.outputTopic.readKeyValue(), (k, v) -> {
        assertEquals(1, k.getMessageId());
        assertEquals(KafkaRecordUtils.MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT, k.getExtendedId());
        assertEquals(2, v.getAnnotationReference().getEntryReferencesCount());
      });

      test.withJournalStore(store -> {
        assertNotNull(store.get(annKey1));
        assertNotNull(store.get(annKey2));
        assertNotNull(store.get(KafkaRecordUtils.annotationsKeyFromMessageId(1L)));
      });
    }
  }

  @Test
  public void deleteCascadesToAllRecordAnnotations() throws Exception {
    try (TestHelper test = new TestHelper(defaultStreamProps())) {

      withAddRecord(1, kv -> test.inputTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey1 = withAnnotateRecord(1, "Ann-1", kv ->
          test.inputTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey2 = withAnnotateRecord(1, "Ann-2", kv ->
          test.inputTopic.pipeInput(kv.key, kv.value));
      withDeleteRecord(1, kv ->
          test.inputTopic.pipeInput(kv.key, kv.value));

      // 2 annotation ref updates,
      // 1 record tombstone,
      // 1 annotation ref tombstone and
      // 2 annotation tombstones
      assertEquals(6, test.outputTopic.getQueueSize());

      test.withJournalStore(store -> {
        assertNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(1)));
        assertNull(store.get(annKey1));
        assertNull(store.get(annKey2));
        assertNull(store.get(KafkaRecordUtils.annotationsKeyFromMessageId(1L)));
      });
    }
  }


  public void withKeyValue(
      KeyValue<JournalEntryKey, JournalEntry> kv,
      BiConsumer<JournalEntryKey, JournalEntry> consumer) {

    assertNotNull(kv);
    consumer.accept(kv.key, kv.value);
  }

  public JournalEntryKey withAnnotateRecord(
      int messageid, String payload, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {

    KeyValue<JournalEntryKey, JournalEntry> kv = createAnnotateRecord(messageid, payload);
    consumer.accept(kv);
    return kv.key;
  }

  public KeyValue<JournalEntryKey, JournalEntry> createAnnotateRecord(
      int messageid, String payload) {

    JournalEntry entry = JournalEntry.newBuilder()
        .setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ANNOTATE_RECORD)
            .setMessageId(messageid)
            .setData(ByteString.copyFrom(payload, StandardCharsets.UTF_8)))
        .build();

    return KeyValue.pair(KafkaRecordUtils.keyFromEntry(entry), entry);
  }

  public void withDeleteRecord(
      int messageid, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {

    consumer.accept(createDeleteRecord(messageid));
  }

  public KeyValue<JournalEntryKey, JournalEntry> createDeleteRecord(int messageid) {

    return KeyValue.pair(
        KafkaRecordUtils.addDeleteKeyFromMessageId(messageid),
        JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.DELETE_RECORD)
            .setMessageId(messageid))
            .build());
  }

  public void withAddRecord(
      int messageid, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {

    consumer.accept(createAddRecord(messageid));
  }

  public Stream<KeyValue<JournalEntryKey, JournalEntry>> createAddRecords(
      int startIdInclusive, int count) {

    return IntStream.range(startIdInclusive, startIdInclusive + count)
        .mapToObj(this::createAddRecord);
  }

  public KeyValue<JournalEntryKey, JournalEntry> createAddRecord(int messageid) {

    return KeyValue.pair(
        KafkaRecordUtils.addDeleteKeyFromMessageId(messageid),
        JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setMessageId(messageid)
            .setProtocolRecordType(ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue())
            .setData(ByteString.copyFrom("Payload-" + messageid, StandardCharsets.UTF_8)))
            .build());
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

  public static class TestHelper implements AutoCloseable {

    static final String journalTopic = "journal-topic";
    final KafkaJournalProcessor processor;
    final TestOutputTopic<JournalEntryKey, JournalEntry> outputTopic;
    final TestInputTopic<JournalEntryKey, JournalEntry> inputTopic;
    final TopologyTestDriver driver;

    public TestHelper(Properties properties) {
      JournalSpec journalSpec = new JournalSpec.Builder()
          .journalName("testJournal")
          .journalTopic(journalTopic)
          .build();
      this.processor = new KafkaJournalProcessor(
          Collections.singletonList(journalSpec), "testNode", properties);

      driver =
          createStreamsTestDriver(
              processor.createTopology(), processor.effectiveStreamProperties());

      inputTopic = driver.createInputTopic(
          journalTopic,
          JournalKeySerde.DEFAULT.serializer(),
          JournalValueSerde.DEFAULT.serializer());

      outputTopic = driver.createOutputTopic(
          journalTopic,
          JournalKeySerde.DEFAULT.deserializer(),
          JournalValueSerde.DEFAULT.deserializer());
    }

    public void withJournalStore(Consumer<KeyValueStore<JournalEntryKey, JournalEntry>> consumer) {
      KeyValueStore<JournalEntryKey, JournalEntry> store = driver
          .getKeyValueStore(processor.getJournals().get(0).storeName());

      consumer.accept(store);
    }

    @Override
    public void close() {
      driver.close();
    }
  }
}
