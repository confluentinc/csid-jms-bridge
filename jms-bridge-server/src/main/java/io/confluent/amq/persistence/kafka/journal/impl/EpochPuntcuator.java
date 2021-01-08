/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.EpochEvent;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

public class EpochPuntcuator implements
    Transformer<JournalEntryKey, JournalEntry, KeyValue<JournalEntryKey, JournalEntry>> {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(EpochCoordinator.class));

  private final KJournal journal;
  private final EpochCoordinator epochCoordinator;

  private Cancellable punchJob = null;
  private ProcessorContext context = null;

  public EpochPuntcuator(KJournal journal,
      EpochCoordinator epochCoordinator) {
    this.journal = journal;
    this.epochCoordinator = epochCoordinator;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;

    //create a punctuation that immediately fires generating an epoch event
    punchJob = context
        .schedule(Duration.ofMillis(10), PunctuationType.WALL_CLOCK_TIME,
            this::punctuate);
  }

  @Override
  public KeyValue<JournalEntryKey, JournalEntry> transform(JournalEntryKey key,
      JournalEntry value) {

    //otherwise pass-through, should never be invoked.
    return KeyValue.pair(key, value);
  }

  @Override
  public void close() {
    if (punchJob != null) {
      punchJob.cancel();
    }
  }

  private void punctuate(long timestamp) {
    int epochId = epochCoordinator.getInitialEpoch();
    SLOG.debug(b -> b
        .event("EpochPunch")
        .putTokens("epochStage", EpochCoordinator.EPOCH_STAGE_START)
        .putTokens("epochId", epochId)
        .putTokens("journalTopic", journal.walTopic())
        .putTokens("partition", this.context.taskId().partition));

    JournalEntry epochEntry = JournalEntry
        .newBuilder()
        .setEpochEvent(EpochEvent.newBuilder()
            .setEpochId(epochId)
            .setEpochStage(EpochCoordinator.EPOCH_STAGE_START)
            .setPartition(this.context.taskId().partition))
        .build();

    this.context.forward(KafkaRecordUtils.epochKey(), epochEntry);

    //epoch has been punched, cancel the job
    this.punchJob.cancel();
  }
}
