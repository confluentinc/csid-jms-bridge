/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.common.base.Stopwatch;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.persistence.kafka.streams.StreamsSupport;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * Built on Kafka Streams, this class is responsible for processing a journal topic.
 * <p>
 * Flows like this:
 * <pre>
 * journalTopic ->
 *    isAppendedRecord:
 *      processTransactions ->
 *      processAddDeleteAnnotate ->
 *        isAddRecord:
 *          processAdd -> STOP //integrate other kafka topics here
 *      publish -> //publish records from resolved TX's, reference aggregates and tombstones
 * journalTopic -> STOP
 * </pre>
 * </p>
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaJournalProcessor implements StateListener {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaJournalProcessor.class));

  private final String journalTopic;
  private final String storeName;

  private final Properties kafkaStreamProps;
  private final KafkaJournalLoader loader;
  private final CountDownLatch streamsReady;

  private KafkaStreams streams;

  public KafkaJournalProcessor(
      String journalTopic,
      Properties kafkaStreamProps) {

    this.streamsReady = new CountDownLatch(1);

    this.journalTopic = journalTopic;
    this.kafkaStreamProps = kafkaStreamProps;
    this.storeName = this.journalTopic + "-store";

    this.loader = new KafkaJournalLoader(this.journalTopic);
  }

  public void init() {
    Topology topology = createTopology();
    SLOG.debug(b -> b
        .name(journalTopic)
        .event("TopologyDescription")
        .message(topology.describe().toString()));

    streams = new KafkaStreams(topology, effectiveStreamProperties());
  }

  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public String getJournalTopic() {
    return journalTopic;
  }

  public String getStoreName() {
    return storeName;
  }

  public Properties effectiveStreamProperties() {
    //default the serdes to our journal entry types.
    Properties kstreamProps = new Properties();
    kstreamProps.putAll(kafkaStreamProps);
    kstreamProps.put(
        StreamsConfig.producerPrefix(ProducerConfig.PARTITIONER_CLASS_CONFIG),
        JournalEntryKeyPartitioner.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        JournalKeySerde.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        JournalValueSerde.class.getCanonicalName());

    return kstreamProps;
  }

  public void startAndLoad(KafkaJournalLoaderCallback callback) {
    if (streams != null) {

      streams.setStateListener(this);
      streams.start();

      waitForStreamReady();
      waitForFullRead(Duration.ofSeconds(10), Duration.ofSeconds(1));

      //grab the state store now that we are loaded
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store = streams
          .store(StoreQueryParameters.fromNameAndType(
              storeName, QueryableStoreTypes.keyValueStore()));

      //finish the callback
      this.loader.executeLoadCallback(store, callback);

    } else {
      throw new IllegalStateException(
          "KafkaJournalProcessor must be initialized before being started.");
    }
  }

  @Override
  public void onChange(State newState, State oldState) {
    if (oldState != State.RUNNING && newState == State.RUNNING) {
      if (streamsReady.getCount() > 0) {
        streamsReady.countDown();
      }
    }
  }

  private void waitForStreamReady() {
    SLOG.info(b -> b
        .name(journalTopic)
        .event("WaitForJournalReady")
        .markStarted());

    try {
      streamsReady.await();
    } catch (Exception e) {
      String err = SLOG.logFormat().build(b -> b
          .name(journalTopic)
          .event("WaitForJournalReady")
          .markFailure());

      throw new RuntimeException(err, e);
    }

    SLOG.info(b -> b
        .name(journalTopic)
        .event("WaitForJournalReady")
        .markCompleted());

  }

  private void waitForFullRead(Duration timeout, Duration delay) {
    SLOG.info(b -> b
        .name(journalTopic)
        .event("WaitForFullJournalRead")
        .markStarted());

    Stopwatch stopwatch = Stopwatch.createStarted();
    while (true) {
      if (timeout.minus(stopwatch.elapsed()).isNegative()) {
        SLOG.info(b -> b
            .name(journalTopic)
            .event("WaitForFullJournalRead")
            .markFailure()
            .putTokens("timeoutSeconds", timeout.getSeconds())
            .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));
        throw new RuntimeException("Failed to fully read journal within the maximum timeout.");
      }

      Map<String, Map<Integer, LagInfo>> lagInfo = streams.allLocalStorePartitionLags();
      boolean allCaughtUp = lagInfo.values().stream()
          .map(Map::entrySet)
          .flatMap(Set::stream)
          .peek(en -> SLOG.trace(b -> b
              .name(journalTopic)
              .event("WaitForFullJournalRead")
              .eventResult("LagInfo")
              .putTokens("partition", en.getKey())
              .putTokens("currentOffsetPosition", en.getValue().currentOffsetPosition())
              .putTokens("endOffsetPosition", en.getValue().endOffsetPosition())
              .putTokens("offsetLag", en.getValue().offsetLag())
          ))
          .allMatch(entry -> entry.getValue().offsetLag() == 0);

      if (allCaughtUp) {
        SLOG.info(b -> b
            .name(journalTopic)
            .event("WaitForFullJournalRead")
            .markCompleted()
            .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));
        break;

      } else {
        try {
          Thread.sleep(delay.toMillis());

          SLOG.info(b -> b
              .name(journalTopic)
              .event("WaitForFullJournalRead")
              .eventResult("InProgress"));

        } catch (Exception e) {
          SLOG.info(b -> b
              .name(journalTopic)
              .event("WaitForFullJournalRead")
              .markFailure()
              .putTokens("timeoutSeconds", timeout.getSeconds())
              .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));
          throw new RuntimeException(e);
        }
      }
    }
  }

  protected Topology createTopology() {
    Properties topoProps = new Properties();
    topoProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    KeyValueBytesStoreSupplier supplier =
        Stores.persistentKeyValueStore(storeName);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<JournalEntryKey, JournalEntry> journalStream = builder
        .table(this.journalTopic,
            Consumed
                .<JournalEntryKey, JournalEntry>as("journalSourceTable")
                .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
            Materialized.<JournalEntryKey, JournalEntry>as(supplier))
        .toStream(Named.as("journalSourceTableToStream"))
        .filter(
            (k, v) -> v != null, Named.as("removeTombstones"))
        .filter(
            (k, v) -> !v.hasAnnotationReference() && !v.hasTransactionReference(),
            Named.as("removeAggregates"));

    KStream<JournalEntryKey, JournalEntry> remaining = StreamsSupport
        .branchStream(journalStream, builder)

        .when((k, v) -> v.hasAppendedRecord())
        .branchTo(this::handleRecordsAndAnnotations)

        .done();

    handleDeadletters(builder, remaining);

    return builder.build(topoProps);
  }

  protected void handleRecordsAndAnnotations(
      StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> recordStream) {

    handleTransactions(sb, recordStream);

  }

  protected void handleTransactions(
      StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> txStream) {

    //allows records to passthough but transates TXs into their records when terminiated
    KStream<JournalEntryKey, JournalEntry> stream = txStream.flatTransform(
        () -> new TransactionProcessor(journalTopic, storeName),
        Named.as("handleTransactions"),
        storeName);

    handleRecords(sb, stream);

  }

  protected void handleRecords(
      StreamsBuilder streamsBuilder, KStream<JournalEntryKey, JournalEntry> recordStream) {

    //branch off the ADD records to other processing
    KStream<JournalEntryKey, JournalEntry> deleteAnnotatStream = StreamsSupport
        .branchStream(recordStream, streamsBuilder)

        .when(appendedRecordTypeIs(JournalRecordType.ADD_RECORD))
        .branchTo(this::handleAddRecord)

        .done();

    handleDeleteAnnotateRecord(streamsBuilder, deleteAnnotatStream);
  }

  /**
   * Override this method to provide functionality around ADD_RECORD processing.
   */
  protected void handleAddRecord(
      StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> recordStream) {

    //this is were messages will be published out to kafka topics
    recordStream.foreach((k, v) -> {
      //do nothing
    }, Named.as("handleAddRecord"));
  }

  protected void handleDeleteAnnotateRecord(
      StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> recordStream) {

    KStream<JournalEntryKey, JournalEntry> stream = recordStream.flatTransform(
        () -> new MainRecordProcessor(journalTopic, storeName),
        Named.as("handleDeleteAnnotateRecord"),
        storeName);

    publishToJournalTopic(stream);
  }

  protected void handleDeadletters(
      StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> dlStream) {

    dlStream.foreach((k, v) ->
        SLOG.error(b -> b
            .event("DeadLetterRecord")
            .message("logging and skipping unprocessable record")
            .name(journalTopic)
            .addJournalEntryKey(k)
            .addJournalEntry(v)), Named.as("handleDeadLetters"));

  }

  protected void publishToJournalTopic(KStream<JournalEntryKey, JournalEntry> entryStream) {
    entryStream.to(journalTopic);
  }

  Predicate<JournalEntryKey, JournalEntry> appendedRecordTypeIs(JournalRecordType rtype) {
    return (k, v) -> v != null && v.getAppendedRecord().getRecordType() == rtype;
  }
}
