/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.persistence.kafka.journal.KJournalState.CREATED;

import com.google.common.base.Stopwatch;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.persistence.kafka.streams.StreamsSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
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
import org.inferred.freebuilder.FreeBuilder;

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

  private final Properties kafkaStreamProps;
  private final CountDownLatch streamsReady;
  private final Map<String, KJournalImpl> journals;
  private final String clientId;

  private volatile KJournalState journalState;
  private volatile KafkaStreams streams;

  public KafkaJournalProcessor(
      List<JournalSpec> journalSpecs,
      String clientId,
      Properties kafkaStreamProps) {

    journals = journalSpecs.stream()
        .map(js -> new KJournalImpl(js, this))
        .collect(Collectors.toMap(KJournalImpl::name, Function.identity()));

    this.clientId = clientId;
    this.streamsReady = new CountDownLatch(1);

    this.kafkaStreamProps = kafkaStreamProps;
    this.journalState = CREATED;
  }

  public boolean isRunning() {
    return journalState.isRunningState();
  }

  public boolean isAssignedPartition(KJournalImpl kjournal, int partition) {
    boolean isAssigned = false;
    if (isRunning()) {
      isAssigned = streams.localThreadsMetadata().stream()
          .flatMap(tm -> tm.activeTasks().stream())
          .flatMap(task -> task.topicPartitions().stream())
          .anyMatch(tp -> kjournal.topic().equals(tp.topic()) && tp.partition() == partition);
    }
    return isAssigned;
  }

  public List<KJournal> getJournals() {
    return new ArrayList<>(journals.values());
  }

  public KJournal getJournal(String name) {
    return journals.get(name);
  }

  public synchronized void start() {
    if (journalState == CREATED) {
      Topology topology = createTopology();
      SLOG.debug(b -> b
          .event("TopologyDescription")
          .message(topology.describe().toString()));

      streams = new KafkaStreams(topology, effectiveStreamProperties());
      streams.setStateListener(this);
      streams.start();

      if (journalState.validTransition(KJournalState.STARTED)) {
        transitionState(KJournalState.STARTED);
      }
    } else {
      SLOG.error(b -> b
          .event("StartJournal")
          .markFailure()
          .putTokens("currentState", journalState)
          .putTokens("nextState", KJournalState.STARTED)
          .message("Invalid state transition"));
    }
  }

  public synchronized void stop() {
    if (journalState.validTransition(KJournalState.STOPPED)) {
      if (streams != null) {
        streams.close();
      }
      SLOG.info(b -> b
          .event("StopJournal")
          .markSuccess()
          .putTokens("currentState", journalState)
          .putTokens("nextState", KJournalState.STOPPED));
      journalState = KJournalState.STOPPED;

    } else {
      SLOG.error(b -> b
          .event("StopJournal")
          .markFailure()
          .putTokens("currentState", journalState)
          .putTokens("nextState", KJournalState.STOPPED)
          .message("Invalid state transition"));
    }
  }


  public Properties effectiveStreamProperties() {
    //default the serdes to our journal entry types.
    Properties kstreamProps = new Properties();
    kstreamProps.putAll(kafkaStreamProps);
    kstreamProps.put(StreamsConfig.CLIENT_ID_CONFIG, "jms-bridge_" + clientId);
    kstreamProps.put(
        StreamsConfig.producerPrefix(ProducerConfig.PARTITIONER_CLASS_CONFIG),
        JournalEntryKeyPartitioner.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        JournalKeySerde.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        JournalValueSerde.class.getCanonicalName());

    return kstreamProps;
  }

  synchronized void load(KJournalImpl kjournal, KafkaJournalLoaderCallback callback) {
    if (streams != null && journalState.isRunningState()) {

      waitForStreamReady();
      waitForFullRead(Duration.ofSeconds(10), Duration.ofSeconds(1));

      //grab the state store now that we are loaded
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store = streams
          .store(StoreQueryParameters.fromNameAndType(
              kjournal.storeName(), QueryableStoreTypes.keyValueStore()));

      //finish the callback
      kjournal.loader().executeLoadCallback(store, callback);

    } else {
      throw new IllegalStateException(
          "KafkaJournalProcessor must be started before being loaded. Current state is: "
              + journalState);
    }
  }

  @Override
  public void onChange(State newState, State oldState) {

    if (oldState != State.RUNNING && newState == State.RUNNING) {
      if (streamsReady.getCount() > 0) {
        streamsReady.countDown();
      }
    }
    transitionState(journalStateFromStreamsState(oldState, newState));
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private KJournalState journalStateFromStreamsState(State oldState, State newState) {
    KJournalState state;

    switch (newState) {
      case CREATED:
        state = CREATED;
        break;
      case REBALANCING:
        state = KJournalState.ASSIGNING;
        break;
      case PENDING_SHUTDOWN:
      case NOT_RUNNING:
        state = KJournalState.STOPPED;
        break;
      case ERROR:
        state = KJournalState.FAILED;
        break;
      case RUNNING:
        if (streamsReady.getCount() == 0) {
          state = KJournalState.RUNNING;
        } else {
          state = KJournalState.LOADING;
        }
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + newState);
    }
    return state;
  }

  private void waitForStreamReady() {
    SLOG.info(b -> b
        .event("WaitForJournalReady")
        .markStarted());

    try {
      streamsReady.await();
    } catch (Exception e) {
      String err = SLOG.logFormat().build(b -> b
          .event("WaitForJournalReady")
          .markFailure());

      throw new RuntimeException(err, e);
    }

    SLOG.info(b -> b
        .event("WaitForJournalReady")
        .markCompleted());

  }

  private void waitForFullRead(Duration timeout, Duration delay) {
    transitionState(KJournalState.LOADING);

    SLOG.info(b -> b
        .event("WaitForFullJournalRead")
        .markStarted());

    int count = 1;
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (true) {
      if (timeout.minus(stopwatch.elapsed()).isNegative()) {
        SLOG.info(b -> b
            .event("WaitForFullJournalRead")
            .markFailure()
            .putTokens("timeoutSeconds", timeout.getSeconds())
            .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));
        throw new RuntimeException("Failed to fully read journal within the maximum timeout.");
      }

      Map<String, Map<Integer, LagInfo>> lagInfo = streams.allLocalStorePartitionLags();
      boolean allCaughtUp = lagInfo.entrySet().stream()
          .peek(topicLagInfo -> {
            final String topic = topicLagInfo.getKey();
            topicLagInfo.getValue().forEach((partition, info) ->
                SLOG.info(b -> b
                    .event("WaitForFullJournalRead")
                    .eventResult("LagInfo")
                    .putTokens("topic", topic)
                    .putTokens("partition", partition)
                    .putTokens("currentOffsetPosition", info.currentOffsetPosition())
                    .putTokens("endOffsetPosition", info.endOffsetPosition())
                    .putTokens("offsetLag", info.offsetLag())
                ));
          })
          .flatMap(topicLagInfo -> topicLagInfo.getValue().entrySet().stream())
          .allMatch(entry -> entry.getValue().offsetLag() == 0);

      if (allCaughtUp) {
        count++;
      }
      if (count >= 2) {
        SLOG.info(b -> b
            .event("WaitForFullJournalRead")
            .markCompleted()
            .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));

        transitionState(journalStateFromStreamsState(streams.state(), streams.state()));
        break;

      } else {
        try {
          Thread.sleep(delay.toMillis());

          SLOG.info(b -> b
              .event("WaitForFullJournalRead")
              .eventResult("InProgress"));

        } catch (Exception e) {
          SLOG.info(b -> b
              .event("WaitForFullJournalRead")
              .markFailure()
              .putTokens("timeoutSeconds", timeout.getSeconds())
              .putTokens("elapsedSeconds", stopwatch.elapsed().getSeconds()));
          transitionState(journalStateFromStreamsState(streams.state(), streams.state()));
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void transitionState(KJournalState newState) {
    SLOG.info(b -> b
        .event("StateTransition")
        .markSuccess()
        .putTokens("currentState", journalState)
        .putTokens("newState", newState));

    journalState = newState;
  }

  protected Topology createTopology() {
    Properties topoProps = new Properties();
    topoProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    final StreamsBuilder builder = new StreamsBuilder();

    journals.values().forEach(j -> addJournalTopology(j, builder));
    return builder.build(topoProps);
  }

  private void addJournalTopology(KJournalImpl kjournal, StreamsBuilder streamsBuilder) {
    KeyValueBytesStoreSupplier supplier =
        Stores.persistentKeyValueStore(kjournal.storeName());

    KStream<JournalEntryKey, JournalEntry> journalStream = streamsBuilder
        .table(kjournal.topic(),
            Consumed
                .<JournalEntryKey, JournalEntry>as(kjournal.prefix("journalSourceTable"))
                .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
            Materialized.<JournalEntryKey, JournalEntry>as(supplier).withCachingDisabled())
        .toStream(Named.as(kjournal.prefix("journalSourceTableToStream")))
        .filter(
            (k, v) -> v != null, Named.as(kjournal.prefix("removeTombstones")))
        .filter(
            (k, v) -> !v.hasAnnotationReference() && !v.hasTransactionReference(),
            Named.as(kjournal.prefix("removeAggregates")));

    KStream<JournalEntryKey, JournalEntry> remaining = StreamsSupport
        .branchStream(journalStream, streamsBuilder)

        .when((k, v) -> v.hasAppendedRecord())
        .branchTo((sb, st) -> this.handleRecordsAndAnnotations(kjournal, sb, st))

        .done();

    handleDeadletters(kjournal, streamsBuilder, remaining);
  }

  protected void handleRecordsAndAnnotations(
      KJournalImpl kjournal, StreamsBuilder sb,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    handleTransactions(kjournal, sb, recordStream);

  }

  protected void handleTransactions(
      KJournalImpl kjournal, StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> txStream) {

    //allows records to passthough but transates TXs into their records when terminiated
    KStream<JournalEntryKey, JournalEntry> stream = txStream.flatTransform(
        () -> new TransactionProcessor(kjournal.topic(), kjournal.storeName()),
        Named.as(kjournal.prefix("handleTransactions")),
        kjournal.storeName());

    handleRecords(kjournal, sb, stream);

  }

  protected void handleRecords(
      KJournalImpl kjournal, StreamsBuilder streamsBuilder,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    //branch off the ADD records to other processing
    KStream<JournalEntryKey, JournalEntry> deleteAnnotatStream = StreamsSupport
        .branchStream(recordStream, streamsBuilder)

        .when(appendedRecordTypeIs(JournalRecordType.ADD_RECORD))
        .branchTo((sb, st) -> this.handleAddRecord(kjournal, sb, st))

        .done();

    handleDeleteAnnotateRecord(kjournal, streamsBuilder, deleteAnnotatStream);
  }

  /**
   * Override this method to provide functionality around ADD_RECORD processing.
   */
  protected void handleAddRecord(
      KJournalImpl kjournal, StreamsBuilder sb,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    //this is were messages will be published out to kafka topics
    recordStream.foreach((k, v) -> {
      //do nothing
    }, Named.as(kjournal.prefix("handleAddRecord")));
  }

  protected void handleDeleteAnnotateRecord(
      KJournalImpl kjournal, StreamsBuilder sb,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    KStream<JournalEntryKey, JournalEntry> stream = recordStream.flatTransform(
        () -> new MainRecordProcessor(kjournal.topic(), kjournal.storeName()),
        Named.as(kjournal.prefix("handleDeleteAnnotateRecord")),
        kjournal.storeName());

    publishToJournalTopic(kjournal, stream);
  }

  protected void handleDeadletters(
      KJournalImpl kjournal, StreamsBuilder sb, KStream<JournalEntryKey, JournalEntry> dlStream) {

    dlStream.foreach((k, v) ->
        SLOG.error(b -> b
            .event("DeadLetterRecord")
            .message("logging and skipping unprocessable record")
            .name(kjournal.name())
            .addJournalEntryKey(k)
            .addJournalEntry(v)), Named.as(kjournal.prefix("handleDeadLetters")));

  }

  protected void publishToJournalTopic(
      KJournalImpl kjournal, KStream<JournalEntryKey, JournalEntry> entryStream) {

    entryStream.to(kjournal.topic());
  }

  Predicate<JournalEntryKey, JournalEntry> appendedRecordTypeIs(JournalRecordType rtype) {
    return (k, v) -> v != null && v.getAppendedRecord().getRecordType() == rtype;
  }

  public String getClientId() {
    return clientId;
  }

  @FreeBuilder
  public interface JournalSpec {

    String journalName();

    String journalTopic();

    class Builder extends KafkaJournalProcessor_JournalSpec_Builder {

    }
  }

  static class KJournalImpl implements KJournal {

    final JournalSpec spec;
    final KafkaJournalProcessor processor;
    final String storeName;
    final KafkaJournalLoader loader;

    KJournalImpl(
        JournalSpec spec,
        KafkaJournalProcessor processor) {

      this.spec = spec;
      this.processor = processor;
      this.storeName = spec.journalTopic() + "-store";
      this.loader = new KafkaJournalLoader(spec.journalName());
    }

    KafkaJournalLoader loader() {
      return loader;
    }

    String prefix(String subject) {
      return storeName + "_" + subject;
    }

    @Override
    public void stop() {
      processor.stop();
    }

    @Override
    public String storeName() {
      return storeName;
    }

    @Override
    public String topic() {
      return spec.journalTopic();
    }

    @Override
    public String name() {
      return spec.journalName();
    }

    @Override
    public void load(KafkaJournalLoaderCallback callback) {
      processor.load(this, callback);
    }

    @Override
    public boolean isAssignedPartition(int partition) {
      return processor.isAssignedPartition(this, partition);
    }

    @Override
    public boolean isRunning() {
      return processor.isRunning();
    }
  }
}
