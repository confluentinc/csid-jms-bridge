/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import org.apache.kafka.common.serialization.Serdes;
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
import org.inferred.freebuilder.FreeBuilder;

import io.confluent.amq.exchange.Headers;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.persistence.kafka.streams.StreamsSupport;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

//CHECKSTYLE:OFF: LineLength

/**
 * Creates the kafka streams topology used to store journal data within Kafka.  It is responsible
 * for maintaining relationships between records and annotations and the execution of transactions.
 * <p>
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


  public static Topology createTopology(TopologySpec topologySpec) {

    StreamsBuilder sb = new StreamsBuilder();
    List<JournalTopology> jts = topologySpec.journals().stream()
        .map(j -> new JournalTopology(
            topologySpec.bridgeId(), j, sb))
        .collect(Collectors.toList());
    jts.forEach(JournalTopology::applyToBuilder);

    return sb.build(topologySpec.topologyProps());
  }

  private final String bridgeId;
  private final KJournal journal;
  private final StreamsBuilder streamsBuilder;

  public JournalTopology(
      String bridgeId,
      KJournal journal,
      StreamsBuilder streamBuilder) {

    this.bridgeId = bridgeId;
    this.journal = journal;
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
    journalStream = journalStream
        .filter((k, v) -> v != null, Named.as(journal.prefix("TombstoneFilter")));

    journalStream = StreamsSupport.branchStream(journalStream, streamsBuilder)
        .when((k, v) -> v.hasEpochEvent() || KafkaRecordUtils.isTxRecord(v.getAppendedRecord()))
        .branchTo(this::handleTransactions)
        .done();

    handleRecords(journalStream, "_Main");

  }

  protected void handleTransactions(
      KStream<JournalEntryKey, JournalEntry> stream) {

    KTable<JournalEntryKey, JournalEntry> txTable = stream
        .toTable(Named.as(journal.prefix("txTable")),
            Materialized.<JournalEntryKey, JournalEntry, KeyValueStore<Bytes, byte[]>>as(
                journal.txStoreName())
                .withCachingDisabled());

    KStream<JournalEntryKey, JournalEntry> txStream = txTable.toStream()
        .flatTransform(() -> new TransactionProcessor(journal.name(), journal.txStoreName()),
            Named.as(journal.prefix("handleTransactions")),
            journal.txStoreName());

    handleRecords(txStream, "_TX");
  }

  protected void handleRecords(
      KStream<JournalEntryKey, JournalEntry> recordStream, String suffix) {

    KStream<JournalEntryKey, JournalEntry> mainStream = recordStream
        .flatTransform(() -> new MainRecordProcessor(journal.name(), journal.storeName()),
            Named.as(journal.prefix("handleMainRecord") + suffix));

    publishToIntegratedKafkaTopics(mainStream, suffix);
    publishToJournalTopic(mainStream);
  }

  protected void publishToIntegratedKafkaTopics(
      KStream<JournalEntryKey, JournalEntry> recordStream, String suffix) {

    recordStream
        .filter((k, v) ->
            v != null
                && v.hasAppendedRecord()
                && KafkaRecordUtils.isMessageRecord(v.getAppendedRecord()))
        .flatTransform(() -> new KafkaDivertProcessor(bridgeId, journal.name()),
            Named.as(journal.prefix("kafkaDivert") + suffix))
        .to((k, v, ctx) -> {
          String topic = Serdes.String().deserializer().deserialize(
              null,
              ctx.headers().lastHeader(Headers.EX_HDR_KAFKA_TOPIC).value());
          ctx.headers().remove(Headers.EX_HDR_KAFKA_TOPIC);
          return topic;
        }, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
  }


  protected void publishToJournalTopic(
      KStream<JournalEntryKey, JournalEntry> entryStream) {

    entryStream.to(journal.tableTopic());
  }

  private void addGlobalStore() {

    String globalStoreName = journal.storeName();
    StoreBuilder<KeyValueStore<JournalEntryKey, JournalEntry>> globalStoreBuilder = Stores
        .keyValueStoreBuilder(
            Stores.persistentKeyValueStore(globalStoreName),
            JournalKeySerde.DEFAULT, JournalValueSerde.DEFAULT)
        .withCachingDisabled();

    streamsBuilder.addGlobalStore(
        globalStoreBuilder,
        journal.tableTopic(),
        Consumed.with(
            JournalKeySerde.DEFAULT,
            JournalValueSerde.DEFAULT),
        () -> new GlobalStoreProcessor(globalStoreName, journal));

  }

  public static class TopologySpecBuilder extends TopologySpec.Builder {

  }

  @SuppressWarnings("InterfaceWithOnlyOneDirectInheritor")
  @FreeBuilder
  public interface TopologySpec {

    String bridgeId();

    Collection<? extends KJournal> journals();

    Properties topologyProps();

    @SuppressWarnings("ClassWithoutConstructor")
    class Builder extends JournalTopology_TopologySpec_Builder {

    }
  }

}
