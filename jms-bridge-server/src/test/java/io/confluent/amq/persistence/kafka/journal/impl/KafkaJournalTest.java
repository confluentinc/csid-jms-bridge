/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.protobuf.ByteString;
import io.confluent.amq.config.BridgeClientId;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecord;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor.JournalSpec;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.test.TestSupport;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.confluent.amq.test.TestSupport.createStreamsTestDriver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Stream processor undergoing changes")
public class KafkaJournalTest {

  @TempDir
  Path tempdir;

  @Test
  public void addRecordsFlowThroughStoreIsCurrent() throws Exception {
    TestHelper test = new TestHelper(TestSupport.baseStreamProps(tempdir));

    createAddRecords(0, 100)
        .forEach(kv -> test.walTopic.pipeInput(kv.key, kv.value));

    assertEquals(100, test.tableTopic.getQueueSize(), "Add records should not flow through!");
    test.withJournalStore(store -> assertEquals(100, store.approximateNumEntries()));
  }

  @Test
  public void deleteCreatesTombstoneUpdatesStore() throws Exception {
    try (TestHelper test = new TestHelper(TestSupport.baseStreamProps(tempdir))) {

      withAddRecord(1, kv -> test.walTopic.pipeInput(kv.key, kv.value));
      withDeleteRecord(1, kv -> test.walTopic.pipeInput(kv.key, kv.value));

      // 1 add record
      // 1 ann refs tombstone
      // 1 record tombstone
      assertEquals(3, test.tableTopic.getQueueSize(), "No tombstones present.");

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(1, k.getMessageId());
        assertEquals(0, k.getExtendedId());
        assertNotNull(v.getAppendedRecord());
        assertEquals(JournalRecordType.ADD_RECORD, v.getAppendedRecord().getRecordType());
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(1, k.getMessageId());
        assertEquals(KafkaRecordUtils.MESSAGE_ANNOTATIONS_EXTENDED_ID_CONSTANT, k.getExtendedId());
        assertNull(v);
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(1, k.getMessageId());
        assertEquals(0, k.getExtendedId());
        assertNull(v);
      });

      test.withJournalStore(store ->
          assertNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(1))));
    }
  }

  @Test
  public void annotationRefsAggregate() throws Exception {
    try (TestHelper test = new TestHelper(TestSupport.baseStreamProps(tempdir))) {

      int msgId = 1;
      final JournalEntryKey addKey = withAddRecord(msgId, kv ->
          test.walTopic.pipeInput(kv.key, kv.value));

      final JournalEntryKey annKey1 = withAnnotateRecord(msgId, "Ann-1", kv ->
          test.walTopic.pipeInput(kv.key, kv.value));

      final JournalEntryKey annKey2 = withAnnotateRecord(msgId, "Ann-2", kv ->
          test.walTopic.pipeInput(kv.key, kv.value));

      assertEquals(5, test.tableTopic.getQueueSize(),
          "Expected 5 records, add, annotate, refs, annotate, and refs");

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(msgId, k.getMessageId());
        assertTrue(v.hasAppendedRecord());
        assertEquals(JournalRecordType.ADD_RECORD, v.getAppendedRecord().getRecordType());
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(msgId, k.getMessageId());
        assertTrue(v.hasAppendedRecord());
        assertEquals(JournalRecordType.ANNOTATE_RECORD, v.getAppendedRecord().getRecordType());
        assertEquals("Ann-1", v.getAppendedRecord().getData().toStringUtf8());
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(msgId, k.getMessageId());
        assertTrue(v.hasAnnotationReference());
        assertEquals(1, v.getAnnotationReference().getEntryReferencesCount());
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(msgId, k.getMessageId());
        assertTrue(v.hasAppendedRecord());
        assertEquals(JournalRecordType.ANNOTATE_RECORD, v.getAppendedRecord().getRecordType());
        assertEquals("Ann-2", v.getAppendedRecord().getData().toStringUtf8());
      });

      withKeyValue(test.tableTopic.readKeyValue(), (k, v) -> {
        assertEquals(msgId, k.getMessageId());
        assertTrue(v.hasAnnotationReference());
        assertEquals(2, v.getAnnotationReference().getEntryReferencesCount());
      });

      test.withJournalStore(store -> {
        assertNotNull(store.get(addKey));
        assertNotNull(store.get(annKey1));
        assertNotNull(store.get(annKey2));
        JournalEntry annRefs = store.get(KafkaRecordUtils.annotationsKeyFromMessageId(1L));
        assertNotNull(annRefs);
        assertTrue(annRefs.hasAnnotationReference());
        assertEquals(2, annRefs.getAnnotationReference().getEntryReferencesCount());
      });
    }
  }

  @Test
  public void deleteCascadesToAllRecordAnnotations() throws Exception {
    try (TestHelper test = new TestHelper(TestSupport.baseStreamProps(tempdir))) {

      withAddRecord(1, kv -> test.walTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey1 = withAnnotateRecord(1, "Ann-1", kv ->
          test.walTopic.pipeInput(kv.key, kv.value));
      final JournalEntryKey annKey2 = withAnnotateRecord(1, "Ann-2", kv ->
          test.walTopic.pipeInput(kv.key, kv.value));
      withDeleteRecord(1, kv ->
          test.walTopic.pipeInput(kv.key, kv.value));

      // 1 add record
      // 2 annotations
      // 2 annotation ref updates,
      // 1 record tombstone,
      // 1 annotation ref tombstone and
      // 2 annotation tombstones
      assertEquals(9, test.tableTopic.getQueueSize());

      test.withJournalStore(store -> {
        assertNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(1)));
        assertNull(store.get(annKey1));
        assertNull(store.get(annKey2));
        assertNull(store.get(KafkaRecordUtils.annotationsKeyFromMessageId(1L)));
      });
    }
  }

  @Test
  public void transactionsExpireAfterTimeout() throws Exception {
    try (TestHelper test = new TestHelper(TestSupport.baseStreamProps(tempdir))) {

      withAddRecord(1,
          asTx(2, kv -> test.walTopic.pipeInput(kv.key, kv.value)));

      final JournalEntryKey annKey1 =
          withAnnotateRecord(3, "Ann-1",
              asTx(2, kv -> test.walTopic.pipeInput(kv.key, kv.value)));

      test.driver.advanceWallClockTime(Duration.ofMinutes(6));
      withAddRecord(4, kv -> test.walTopic.pipeInput(kv.key, kv.value));
      withCommitRecord(2, kv -> test.walTopic.pipeInput(kv.key, kv.value));

      // 1 epoch event
      // 1 add message
      assertEquals(2, test.tableTopic.getQueueSize());

      test.withJournalStore(store -> {
        assertEquals(1, store.approximateNumEntries());
        assertNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(1)));
        assertNull(store.get(annKey1));
        assertNotNull(store.get(KafkaRecordUtils.addDeleteKeyFromMessageId(4)));
      });

      test.withTxJournalStore(store -> {
        assertEquals(0, store.approximateNumEntries());
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

  public void withCommitRecord(
      int txId, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {

    consumer.accept(createCommitRecord(txId));
  }

  public KeyValue<JournalEntryKey, JournalEntry> createCommitRecord(long txId) {

    return KeyValue.pair(
        KafkaRecordUtils.transactionKeyFromTxId(txId),
        JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.COMMIT_TX)
            .setTxId(txId))
            .build());
  }

  public JournalEntryKey withAddRecord(
      int messageid, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {

    KeyValue<JournalEntryKey, JournalEntry> kv = createAddRecord(messageid);
    consumer.accept(kv);
    return kv.key;
  }

  public Stream<KeyValue<JournalEntryKey, JournalEntry>> createAddRecords(
      int startIdExclusive, int count) {

    return IntStream.range(startIdExclusive, startIdExclusive + count + 1)
        .mapToObj(this::createAddRecord);
  }

  public KeyValue<JournalEntryKey, JournalEntry> createAddRecord(int messageid) {

    KeyValue<JournalEntryKey, JournalEntry> kv = KeyValue.pair(
        KafkaRecordUtils.addDeleteKeyFromMessageId(messageid),
        JournalEntry.newBuilder().setAppendedRecord(JournalRecord.newBuilder()
            .setRecordType(JournalRecordType.ADD_RECORD)
            .setMessageId(messageid)
            .setProtocolRecordType(ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue())
            .setData(ByteString.copyFrom("Payload-" + messageid, StandardCharsets.UTF_8)))
            .build());
    return kv;
  }

  public Consumer<KeyValue<JournalEntryKey, JournalEntry>> asTx(
      int txId, Consumer<KeyValue<JournalEntryKey, JournalEntry>> consumer) {
    return kv -> {

      JournalRecordType txType = null;
      switch (kv.value.getAppendedRecord().getRecordType()) {
        case ADD_RECORD:
          txType = JournalRecordType.ADD_RECORD_TX;
          break;
        case DELETE_RECORD:
          txType = JournalRecordType.DELETE_RECORD_TX;
          break;
        case ANNOTATE_RECORD:
          txType = JournalRecordType.ANNOTATE_RECORD_TX;
          break;
        default:
          break;
      }

      if (txType == null) {
        consumer.accept(kv);

      } else {
        KeyValue newKv = KeyValue.pair(
            JournalEntryKey.newBuilder(kv.key).setTxId(txId).build(),
            JournalEntry.newBuilder()
                .setAppendedRecord(JournalRecord.newBuilder(kv.value.getAppendedRecord())
                    .setTxId(txId)
                    .setRecordType(txType))
                .build());
        consumer.accept(newKv);

      }
    };
  }


  public static class TestHelper implements AutoCloseable {

    static final String journalWalTopic = "journal_topic_wal";
    static final String journalTableTopic = "journal_topic_tbl";
    final KafkaJournalProcessor processor;
    final TestInputTopic<JournalEntryKey, JournalEntry> walTopic;
    final TestOutputTopic<JournalEntryKey, JournalEntry> tableTopic;
    final TopologyTestDriver driver;
    final KafkaIO mockKafkaIo;

    public TestHelper(Map<String, String> props) {
      final JournalSpec journalSpec = new JournalSpec.Builder()
          .journalName("testJournal")
          .mutateJournalWalTopic(t -> t
              .partitions(1)
              .replication(1)
              .name(journalWalTopic))
          .mutateJournalTableTopic(t -> t
              .partitions(1)
              .replication(1)
              .name(journalTableTopic))
          .build();

      final BridgeConfig bridgeConfig = new BridgeConfig.Builder()
          .id("testBridge")
          .putAllStreams(props)
          .buildPartial();

      mockKafkaIo = Mockito.mock(KafkaIO.class, Mockito.RETURNS_DEFAULTS);
      Map<String, TopicDescription> topicDescriptions = new HashMap<>();
      topicDescriptions.put(
          journalWalTopic,
          new TopicDescription(
              journalWalTopic,
              false,
              Arrays.asList(new TopicPartitionInfo(
                  0, null, Collections.emptyList(), Collections.emptyList()))));
      topicDescriptions.put(
          journalTableTopic,
          new TopicDescription(
              journalTableTopic,
              false,
              Arrays.asList(new TopicPartitionInfo(
                  0, null, Collections.emptyList(), Collections.emptyList()))));

      Mockito.when(mockKafkaIo.describeTopics(Mockito.anyCollection()))
          .thenReturn(topicDescriptions);

      this.processor = new KafkaJournalProcessor(
          bridgeConfig.id(),
          Collections.singletonList(journalSpec),
          new BridgeClientId.Builder().bridgeId("testNode").buildPartial(),
          "testApp",
          Duration.ofSeconds(60),
          bridgeConfig.streams(), mockKafkaIo);

      this.processor.initializeJournals();
      driver =
          createStreamsTestDriver(
              processor.createTopology(), processor.effectiveStreamProperties());

      walTopic = driver.createInputTopic(
          journalWalTopic,
          JournalKeySerde.DEFAULT.serializer(),
          JournalValueSerde.DEFAULT.serializer());

      tableTopic = driver.createOutputTopic(
          journalTableTopic,
          JournalKeySerde.DEFAULT.deserializer(),
          JournalValueSerde.DEFAULT.deserializer());
    }

    public void withTxJournalStore(
        Consumer<KeyValueStore<JournalEntryKey, JournalEntry>> consumer) {

      KeyValueStore<JournalEntryKey, JournalEntry> store = driver
          .getKeyValueStore(processor.getJournals().get(0).txStoreName());

      consumer.accept(store);
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
