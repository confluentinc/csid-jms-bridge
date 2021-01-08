/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor.JournalSpec;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestRecordSupport;
import io.confluent.amq.test.TestSupport;
import io.confluent.amq.test.TestSupport.LoaderCallbackData;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JournalLoadTest {

  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.5.2")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  KafkaIO kafkaIO;
  JournalSpec journalSpec;
  KafkaJournalProcessor processor;

  @BeforeEach
  public void beforeEach() {
    kafkaIO = new KafkaIO(kafkaContainer.defaultProps());
    kafkaIO.start();

    String journalWalTopic = kafkaContainer.safeCreateTopic("journal_wal", 4);
    String journalTableTopic = kafkaContainer.safeCreateTopic("journal_tbl", 4);
    journalSpec = new JournalSpec.Builder()
        .journalName("testJournal")
        .mutateJournalWalTopic(t -> t
            .partitions(4)
            .replication(1)
            .name(journalWalTopic))
        .mutateJournalTableTopic(t -> t
            .partitions(4)
            .replication(1)
            .name(journalTableTopic))
        .build();

    processor = new KafkaJournalProcessor(
        Collections.singletonList(journalSpec),
        "test-client",
        "test-app",
        TestSupport.baseStreamProps(tempdir, Maps.fromProperties(kafkaContainer.defaultProps())),
        kafkaIO);
  }

  @AfterEach
  public void afterEach() {
    if (kafkaIO != null) {
      kafkaIO.stop();
    }

    if (processor != null) {
      processor.stop();
    }
  }

  @Test
  public void testLoadWithOpenTx() throws Exception {
    KeyValue<JournalEntryKey, JournalEntry> txPrep = KeyValue.pair(
        TestRecordSupport.makeTxKey(1001L, 2000L),
        TestRecordSupport.makeRecordOfType(
            1001L, 2000L, null, JournalRecordType.PREPARE_TX, "prep"));

    KeyValue<JournalEntryKey, JournalEntry> txAdd = KeyValue.pair(
        TestRecordSupport.makeTxKey(1001L, 2001L),
        TestRecordSupport.makeRecordOfType(
            1001L, 2001L, null, JournalRecordType.ADD_RECORD_TX, "test"));

    KeyValue<JournalEntryKey, JournalEntry> add = KeyValue.pair(
        TestRecordSupport.makeMsgKey(2002L),
        TestRecordSupport.makeRecordOfType(
            null, 2002L, null, JournalRecordType.ADD_RECORD));

    kafkaIO.withProducer(p -> {
      try {
        p.send(new ProducerRecord<>(journalSpec.journalWalTopic().name(), txPrep.key, txPrep.value))
            .get();
        p.send(new ProducerRecord<>(journalSpec.journalWalTopic().name(), txAdd.key, txAdd.value))
            .get();
        p.send(new ProducerRecord<>(journalSpec.journalWalTopic().name(), add.key, add.value))
            .get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    processor.start();
    KJournal journal = processor.getJournal(journalSpec.journalName());
    LoaderCallbackData loaderCallback = TestSupport.createLoaderCallback();
    journal.load(loaderCallback.callback);
    assertEquals(1, loaderCallback.committedRecords.size());
    assertTrue(loaderCallback.committedRecords.stream().anyMatch(r -> r.id == 2002L));
    assertEquals(0, loaderCallback.failedTxids.size());
    assertEquals(1, loaderCallback.preppedTxs.size());
    assertEquals(1001L, loaderCallback.preppedTxs.get(0).getId());

    TestSupport.retry(5, 100, () ->
        assertEquals(KJournalState.RUNNING, processor.currentState()));
  }

  @Test
  public void testLoadWithClosedTx() throws Exception {
    KeyValue<JournalEntryKey, JournalEntry> txAdd = KeyValue.pair(
        TestRecordSupport.makeTxKey(1001L, 2001L),
        TestRecordSupport.makeRecordOfType(
            1001L, 2001L, null, JournalRecordType.ADD_RECORD_TX, "test"));

    KeyValue<JournalEntryKey, JournalEntry> add = KeyValue.pair(
        TestRecordSupport.makeMsgKey(2002L),
        TestRecordSupport.makeRecordOfType(
            null, 2002L, null, JournalRecordType.ADD_RECORD));

    KeyValue<JournalEntryKey, JournalEntry> txCommit = KeyValue.pair(
        TestRecordSupport.makeTxKey(1001L),
        TestRecordSupport.makeRecordOfType(
            1001L, null, null, JournalRecordType.COMMIT_TX));

    kafkaIO.withProducer(p -> {
      try {
        p.send(new ProducerRecord<>(journalSpec.journalWalTopic().name(), txAdd.key, txAdd.value))
            .get();
        p.send(new ProducerRecord<>(journalSpec.journalWalTopic().name(), add.key, add.value))
            .get();
        p.send(new ProducerRecord<>(
            journalSpec.journalWalTopic().name(), txCommit.key, txCommit.value)).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    processor.start();
    KJournal journal = processor.getJournal(journalSpec.journalName());
    LoaderCallbackData loaderCallback = TestSupport.createLoaderCallback();
    journal.load(loaderCallback.callback);
    assertEquals(2, loaderCallback.committedRecords.size());
    assertTrue(loaderCallback.committedRecords.stream().anyMatch(r -> r.id == 2002L));
    assertTrue(loaderCallback.committedRecords.stream().anyMatch(r -> r.id == 2001L));

    TestSupport.retry(5, 100, () ->
        assertEquals(KJournalState.RUNNING, processor.currentState()));

    System.out.println(kafkaContainer.listTopics());
  }
}
