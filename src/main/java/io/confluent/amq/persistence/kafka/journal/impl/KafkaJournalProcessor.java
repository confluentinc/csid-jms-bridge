/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import io.confluent.amq.persistence.kafka.ReconciledMessage.ReconciledMessageSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;

/**
 * Built on Kafka Streams, this class is responsible for processing a journal topic.
 */
public class KafkaJournalProcessor {

  private final String journalTopic;
  private final String storeName;

  private final Properties kafkaStreamProps;
  private final KafkaJournalStoreLoader loader;
  private final KafkaJournalReconciler reconciler;
  private final KafkaJournalTxHandler txHandler;

  private KafkaStreams streams;

  public KafkaJournalProcessor(
      String journalTopic,
      Properties kafkaStreamProps) {

    this.journalTopic = journalTopic;
    this.kafkaStreamProps = kafkaStreamProps;
    this.storeName = this.journalTopic + "-store";
    this.txHandler = new KafkaJournalTxHandler(this.journalTopic);
    this.loader = new KafkaJournalStoreLoader(this.storeName, this.txHandler);

    //For future release
    //this.reconciler = new KafkaJournalReconciler(kafkaSyncHandler, this.txHandler, journalTopic);

    this.reconciler = new KafkaJournalReconciler(this.txHandler, journalTopic);
  }

  public void init() {
    Properties topoProps = new Properties();
    topoProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    streams = new KafkaStreams(createTopology(topoProps), kafkaStreamProps);
    streams.setGlobalStateRestoreListener(this.loader);
  }

  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public void startAndLoad(KafkaJournalLoaderCallback callback) {
    if (streams != null) {
      this.loader.readyLoader(callback);
      streams.setStateListener(this.loader);
      streams.start();
    } else {
      throw new IllegalStateException(
          "KafkaJournalProcessor must be initialized before being started.");
    }
  }

  protected Topology createTopology(Properties topoProps) {
    Serde<JournalRecord> journalRecordSerde = Serdes
        .serdeFrom(new ProtoSerializer<>(), new ProtoDeserializer<>(JournalRecord::parseFrom));
    Serde<JournalRecordKey> journalRecordKeySerde = Serdes
        .serdeFrom(new ProtoSerializer<>(), new ProtoDeserializer<>(JournalRecordKey::parseFrom));

    KeyValueBytesStoreSupplier supplier =
        KafkaJournalStoreLoader.createSupplier(this.loader);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<JournalRecordKey, JournalRecord> journalStream = builder.table(this.journalTopic,
        Consumed.with(journalRecordKeySerde, journalRecordSerde)
            .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
        Materialized.<JournalRecordKey, JournalRecord>as(supplier).withCachingDisabled().withLoggingDisabled())
        .toStream();

        //main processing route
        journalStream.flatMapValues(this.reconciler::reconcileRecord)
        .to(journalTopic,
            Produced.with(Serdes.ByteArray(), new ReconciledMessageSerde())
              .withStreamPartitioner(new JournalRecordKeyPartitioner())));

        //side processing
        // journalStream.filter(adds only).to(external kafka topic)

    return builder.build(topoProps);
  }

  private JournalRecord deserializeJournalRecord(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try {
      return JournalRecord.parseFrom(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
