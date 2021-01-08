/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;


//CHECKSTYLE:OFF: LineLength
/**
 * Creates the kafka streams topology used to store journal data within Kafka.  It is responsible
 * for maintaining relationships between records and annotations and the execution of transactions.
 *
 * Below is a sequence diagram illustrating how messages generally flow through it.
 * <pre>
 * +---------+                   +-----------+                       +-------------+            +-----------------+
 * | Journal |                   | WalTopic  |                       | TxTblTopic  |            | RecordTblTopic  |
 * +---------+                   +-----------+                       +-------------+            +-----------------+
 *      |                              |                                    |                            |
 *      | Add record                   |                                    |                            |
 *      |----------------------------->|                                    |                            |
 *      |                              |                                    |                            |
 *      |                              | Insert record                      |                            |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      | Annotate target record       |                                    |                            |
 *      |----------------------------->|                                    |                            |
 *      |                              |                                    |                            |
 *      |                              | Insert annotation record           |                            |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      |                              | Update annotation references for target record                  |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      | Delete record                |                                    |                            |
 *      |----------------------------->|                                    |                            |
 *      |                              |                                    |                            |
 *      |                              | Delete record's annotation references                           |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      |                              | Delete annotation records          |                            |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      |                              | Delete record from table           |                            |
 *      |                              |---------------------------------------------------------------->|
 *      |                              |                                    |                            |
 *      | TX Add record                |                                    |                            |
 *      |----------------------------->|                                    |                            |
 *      |                              |                                    |                            |
 *      |                              | Insert TX Add record               |                            |
 *      |                              |----------------------------------->|                            |
 *      |                              |                                    |                            |
 *      |                              | Upsert TX record references        |                            |
 *      |                              |----------------------------------->|                            |
 *      |                              |                                    |                            |
 *      | TX Commit                    |                                    |                            |
 *      |----------------------------->|                                    |                            |
 *      |                              |                                    |                            |
 *      |                              | Commit TX                          |                            |
 *      |                              |----------------------------------->|                            |
 *      |                              |                                    |                            |
 *      |                              |                                    | Add Record                 |
 *      |                              |                                    |--------------------------->|
 *      |                              |                                    |                            |
 *      |                              |                                    | Delete TX records          |
 *      |                              |                                    |------------------          |
 *      |                              |                                    |                 |          |
 *      |                              |                                    |<-----------------          |
 *      |                              |                                    |                            |
 * </pre>
 */
//CHECKSTYLE:ON: LineLength
public class JournalTopology {

  public static Topology createTopology(
      Collection<? extends KJournal> journals, EpochCoordinator coordinator, Properties topoProps) {

    StreamsBuilder sb = new StreamsBuilder();
    List<JournalTopology> jts = journals.stream()
        .map(j -> new JournalTopology(j, sb, coordinator))
        .collect(Collectors.toList());
    jts.forEach(JournalTopology::applyToBuilder);

    Topology topo = sb.build(topoProps);
    return topo;
  }

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(JournalTopology.class));

  private final KJournal journal;
  private final EpochCoordinator coordinator;
  private final StreamsBuilder streamsBuilder;

  public JournalTopology(KJournal journal, StreamsBuilder streamBuilder,
      EpochCoordinator coordinator) {

    this.journal = journal;
    this.coordinator = coordinator;
    this.streamsBuilder = streamBuilder;
  }

  protected void applyToBuilder() {
    KStream<JournalEntryKey, JournalEntry> journalStream = streamsBuilder
        .stream(
            journal.walTopic(),
            Consumed
                .<JournalEntryKey, JournalEntry>as(journal.prefix("journalSourceTable"))
                .withOffsetResetPolicy(AutoOffsetReset.EARLIEST));

    addGlobalStore();
    addEpochPunch(journalStream);
    journalStream = journalStream
        .filter((k, v) -> v != null, Named.as(journal.prefix("TombstoneFilter")));

    journalStream = handleTransactions(journalStream);
    journalStream = handleRecords(journalStream);
    publishToJournalTopic(journalStream);

  }

  private void addEpochPunch(KStream<JournalEntryKey, JournalEntry> journalStream) {
    journalStream
        //to prevent all messages from going here, we simply want to punctuate the stream.
        .filter((k, v) -> false)
        .transform(() -> new EpochPuntcuator(journal, coordinator))
        .to(journal.walTopic(), Produced
            .with(JournalKeySerde.DEFAULT, JournalValueSerde.DEFAULT)
            .withStreamPartitioner((t, k, v, numPartitions) ->
                //make sure the epoch event gets on the partition that it was meant for
                v.getEpochEvent().getPartition()
            ));

  }

  protected KStream<JournalEntryKey, JournalEntry> handleTransactions(
      KStream<JournalEntryKey, JournalEntry> stream) {

    KTable<JournalEntryKey, JournalEntry> txTable = stream
        .toTable(Named.as(journal.prefix("txTable")),
            Materialized.<JournalEntryKey, JournalEntry, KeyValueStore<Bytes, byte[]>>as(
                journal.txStoreName())
                .withCachingDisabled());

    return txTable.toStream()
        .flatTransform(() -> new TransactionProcessor(journal.walTopic(), journal.txStoreName()),
            Named.as(journal.prefix("handleTransactions")),
            journal.txStoreName());
  }

  protected KStream<JournalEntryKey, JournalEntry> handleRecords(
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    return recordStream
        .flatTransform(() -> new MainRecordProcessor(journal.walTopic(), journal.storeName()),
            Named.as(journal.prefix("handleMainRecord")));
  }

  protected void handleDeadletters(
      KJournal kjournal, StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> dlStream) {

    dlStream.foreach((k, v) ->
        SLOG.error(b -> b
            .event("DeadLetterRecord")
            .message("logging and skipping unprocessable record")
            .name(kjournal.name())
            .addJournalEntryKey(k)
            .addJournalEntry(v)), Named.as(kjournal.prefix("handleDeadLetters")));

  }

  protected void publishToJournalTopic(
      KStream<JournalEntryKey, JournalEntry> entryStream) {

    entryStream.to(journal.tableTopic());
  }

  private void addGlobalStore() {

    final String globalStoreName = journal.storeName();
    StoreBuilder<KeyValueStore<JournalEntryKey, JournalEntry>> globalStoreBuilder = Stores
        .keyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore(globalStoreName),
            JournalKeySerde.DEFAULT, JournalValueSerde.DEFAULT)
        .withLoggingDisabled();

    streamsBuilder.addGlobalStore(
        globalStoreBuilder,
        journal.tableTopic(),
        Consumed.with(
            JournalKeySerde.DEFAULT,
            JournalValueSerde.DEFAULT),
        () -> new GlobalStoreProcessor(globalStoreName, journal));

  }

}
