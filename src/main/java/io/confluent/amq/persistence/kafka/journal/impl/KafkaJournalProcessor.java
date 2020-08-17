/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
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
import org.apache.kafka.streams.kstream.Produced;
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
 *    isAppendedRecord or isAnnotation:
 *    |  isTransaction:
 *    |  |  isTransaction_Complete: -> publish records and tombstones -> journalTopic
 *    |  |  |
 *    |  |  isTransaction_NotComplete: -> keep aggregate list of transaction records by TXID
 *    |  isNotTransaction:
 *    |  |  isAnnotation or isAppendedRecord_Annotation:
 *    |  |  |
 *    |  |  isAppendedRecord_Annotation: -> convert to applied annotation
 *    |  |  |
 *    |  |  isAnnotation -> aggregate annotations and publish the result back -> journalTopic
 *    |  isNotAnnotationRecord:
 *    |     isAppendedRecord_Add: -> convert to message and publish -> journalTopic
 *    |     |
 *    |     isAppendedRecord_Delete: -> create tombstones, message and annotations -> journalTopic
 *    isMessage: -> forward on
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
    Properties topoProps = new Properties();
    topoProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    Topology topology = createTopology(topoProps);
    SLOG.debug(b -> b
        .name(journalTopic)
        .event("TopologyDescription")
        .message(topology.describe().toString()));

    //default the serdes to our journal entry types.
    Properties kstreamProps = new Properties();
    kstreamProps.putAll(kafkaStreamProps);
    kstreamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        JournalKeySerde.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        JournalValueSerde.class.getCanonicalName());

    streams = new KafkaStreams(topology, kstreamProps);
  }

  public void stop() {
    if (streams != null) {
      streams.close();
    }
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

  protected Topology createTopology(Properties topoProps) {
    KeyValueBytesStoreSupplier supplier =
        Stores.persistentKeyValueStore(storeName);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<JournalEntryKey, JournalEntry> journalStream = builder.table(this.journalTopic,
        Consumed
            .<JournalEntryKey, JournalEntry>as("journalSourceTable")
            .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
        Materialized.<JournalEntryKey, JournalEntry>as(supplier))
        .toStream(Named.as("journalSourceTableToStream"))
        .filter((k, v) -> v != null, Named.as("removeTombstones"))
        .filter((k, v) -> !v.hasAnnotationReference() && !v.hasTransactionReference(),
            Named.as("removeAggregates"));

    KStream<JournalEntryKey, JournalEntry> remaining = branchStream(journalStream, builder)

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
    KStream<JournalEntryKey, JournalEntry> stream =
        txStream.flatTransform(() -> new TransactionProcessor(journalTopic, storeName),
            Named.as("handleTransactions"),
            storeName);

    handleRecords(sb, stream);

  }

  protected void handleRecords(
      StreamsBuilder streamsBuilder, KStream<JournalEntryKey, JournalEntry> recordStream) {

    //branch off the ADD records to other processing
    KStream<JournalEntryKey, JournalEntry> deleteAnnotatStream =
        branchStream(recordStream, streamsBuilder)
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

    KStream<JournalEntryKey, JournalEntry> stream = recordStream
        .flatTransform(
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
    Produced<JournalEntryKey, JournalEntry> journalProducedSpec = Produced
        .streamPartitioner(new JournalEntryKeyPartitioner());

    entryStream
        .to(journalTopic, journalProducedSpec);
  }

  Predicate<JournalEntryKey, JournalEntry> appendedRecordTypeIs(JournalRecordType rtype) {
    return (k, v) ->
        v != null
            && v.hasAppendedRecord()
            && v.getAppendedRecord() != null
            && v.getAppendedRecord().getRecordType() == rtype;
  }

  /**
   * Apply given functions to the stream when the predicate is met. Each predicate should be
   * mutually exclusive since the first one passing will be the one used.
   * <p>
   * Returns the stream of entries that do not match any of the given predicates.
   * </p><p>
   * See {@link KStream#branch(Predicate[])}.
   * </p>
   *
   * @param stream the KStream to branch from
   * @param sb     used to add stores to the topology if needed.
   * @return the stream of entries that do not match any of the given predicate executions.
   */
  protected <K, V> BranchStreamBuilder<K, V> branchStream(
      KStream<K, V> stream,
      StreamsBuilder sb) {

    return new BranchStreamBuilder<>(stream, sb);
  }

  interface BranchSpec<K, V> {

    BranchStreamSpec<K, V> branchTo(BiConsumer<StreamsBuilder, KStream<K, V>> executor);
  }

  interface BranchStreamSpec<K, V> {

    BranchSpec<K, V> when(Predicate<K, V> predicate);

    KStream<K, V> done();
  }

  static class JournalBranchExecution<K, V> {

    final Predicate<K, V> predicate;
    final BiConsumer<StreamsBuilder, KStream<K, V>> execution;

    JournalBranchExecution(
        Predicate<K, V> predicate,
        BiConsumer<StreamsBuilder, KStream<K, V>> execution) {

      this.predicate = predicate;
      this.execution = execution;
    }
  }

  static class BranchStreamBuilder<K, V> implements BranchSpec<K, V>, BranchStreamSpec<K, V> {

    private final List<JournalBranchExecution<K, V>> execs = new ArrayList<>();
    private final KStream<K, V> stream;
    private final StreamsBuilder streamsBuilder;

    private Predicate<K, V> currPredicate;

    BranchStreamBuilder(KStream<K, V> stream,
        StreamsBuilder streamsBuilder) {

      this.stream = stream;
      this.streamsBuilder = streamsBuilder;
    }

    @Override
    public BranchStreamSpec<K, V> branchTo(BiConsumer<StreamsBuilder, KStream<K, V>> executor) {
      Preconditions.checkState(currPredicate != null,
          "When must be called prior to branchTo with a non-null predicate.");

      Preconditions.checkArgument(executor != null,
          "branchTo requires a non-null execution target.");

      execs.add(new JournalBranchExecution<>(currPredicate, executor));
      currPredicate = null;

      return this;
    }

    @Override
    public BranchSpec<K, V> when(Predicate<K, V> predicate) {
      currPredicate = predicate;
      return this;
    }

    @Override
    public KStream<K, V> done() {
      if (execs.isEmpty()) {
        return stream;
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      Predicate<K, V>[] predicates =
          new Predicate[execs.size() + 1];

      for (int i = 0; i < execs.size(); i++) {
        predicates[i] = execs.get(i).predicate;
      }
      predicates[execs.size()] = (k, v) -> true;

      KStream<K, V>[] branches = stream
          .branch(predicates);

      for (int i = 0; i < execs.size(); i++) {
        execs.get(i).execution.accept(streamsBuilder, branches[i]);
      }
      return branches[execs.size()];
    }
  }
}
