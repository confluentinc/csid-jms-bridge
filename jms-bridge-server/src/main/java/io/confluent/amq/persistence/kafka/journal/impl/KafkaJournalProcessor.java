/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import static io.confluent.amq.persistence.kafka.journal.KJournalState.CREATED;
import static io.confluent.amq.persistence.kafka.journal.KJournalState.STARTED;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.persistence.kafka.streams.StreamsSupport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
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

  private final String clientId;
  private final String applicationId;
  private final Map<String, String> streamsConfig;
  private final KafkaIO kafkaIO;

  private final EpochCoordinator epochCoordinator;
  private final Map<String, KJournalImpl> journals;

  private volatile KJournalState journalState;
  private volatile KafkaStreams streams;
  private volatile boolean loadComplete = false;

  public KafkaJournalProcessor(
      List<JournalSpec> journalSpecs,
      String clientId,
      String applicationId,
      Map<String, String> streamsConfig,
      KafkaIO kafkaIO) {

    this(journalSpecs, clientId, applicationId, streamsConfig, kafkaIO, new EpochCoordinator());
  }

  protected KafkaJournalProcessor(
      List<JournalSpec> journalSpecs,
      String clientId,
      String applicationId,
      Map<String, String> streamsConfig,
      KafkaIO kafkaIO,
      EpochCoordinator epochCoordinator) {

    this.clientId = clientId;
    this.applicationId = applicationId;
    this.streamsConfig = streamsConfig;
    this.kafkaIO = kafkaIO;

    this.epochCoordinator = epochCoordinator;
    this.journalState = CREATED;

    CompletableFuture[] loadingfutures = new CompletableFuture[journalSpecs.size()];
    journals = ensureTopics(journalSpecs).stream()
        .map(js -> new KJournalImpl(js, this, epochCoordinator))
        .collect(Collectors.toMap(KJournalImpl::name, Function.identity()));

    int idx = 0;
    for (KJournalImpl j : journals.values()) {
      loadingfutures[idx] = j.loader().onLoadComplete();
      idx++;
    }
    CompletableFuture.allOf(loadingfutures).whenComplete((nil, t) -> {
      if (t != null) {
        SLOG.error(b -> b
            .event("LoadListener")
            .markFailure()
            .message("Loading of a journal failed."), t);
      }
      loadComplete = true;
      if (journalState == KJournalState.LOADING) {
        transitionState(KJournalState.RUNNING);
      }
    });


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
          .anyMatch(tp -> kjournal.walTopic().equals(tp.topic()) && tp.partition() == partition);
    }
    return isAssigned;
  }

  public List<KJournal> getJournals() {
    return new ArrayList<>(journals.values());
  }

  public KJournal getJournal(String name) {
    return journals.get(name);
  }

  public KJournalState currentState() {
    return this.journalState;
  }

  public synchronized void start() {
    if (journalState.validTransition(STARTED)) {
      Topology topology = createTopology();
      SLOG.debug(b -> b
          .event("TopologyDescription")
          .message(topology.describe().toString()));

      streams = new KafkaStreams(topology, effectiveStreamProperties(), this.epochCoordinator);
      streams.setStateListener(this);
      streams.start();

      //may have already gone to assigning
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

  protected Collection<JournalSpec> ensureTopics(Collection<JournalSpec> journals) {
    Set<String> topics = kafkaIO.listTopics();
    journals.forEach(jspec -> {
      if (!topics.contains(jspec.journalTableTopic().name())) {
        kafkaIO.createTopic(
            jspec.journalTableTopic().name(),
            jspec.journalTableTopic().partitions(),
            jspec.journalTableTopic().replication(),
            jspec.journalTableTopic().configs());
      }

      if (!topics.contains(jspec.journalWalTopic().name())) {
        kafkaIO.createTopic(
            jspec.journalWalTopic().name(),
            jspec.journalWalTopic().partitions(),
            jspec.journalWalTopic().replication(),
            jspec.journalWalTopic().configs());
      }
    });
    Map<String, TopicDescription> topicDescriptions =
        kafkaIO.describeTopics(journals.stream()
            .flatMap(jspec ->
                Stream.of(jspec.journalWalTopic().name(), jspec.journalTableTopic().name()))
            .collect(Collectors.toSet()));

    return journals.stream()
        .map(jspec -> {
          TopicDescription walTopic = topicDescriptions.get(jspec.journalWalTopic().name());
          TopicDescription tblTopic = topicDescriptions.get(jspec.journalTableTopic().name());
          return new JournalSpec.Builder().mergeFrom(jspec)
              .mutateJournalWalTopic(t -> t
                  .partitions(walTopic.partitions().size())
                  .replication(walTopic.partitions().get(0).replicas().size()))
              .mutateJournalTableTopic(t -> t
                  .partitions(tblTopic.partitions().size())
                  .replication(tblTopic.partitions().get(0).replicas().size()))
              .build();
        })
        .collect(Collectors.toList());
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
    Properties kstreamProps = new Properties();
    kstreamProps.putAll(streamsConfig);
    kstreamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    kstreamProps.put(StreamsConfig.CLIENT_ID_CONFIG, "jms-bridge_" + clientId);

    //ensure our custom partitioner is used since it should depend on partial key values only
    kstreamProps.put(
        StreamsConfig.producerPrefix(ProducerConfig.PARTITIONER_CLASS_CONFIG),
        JournalEntryKeyPartitioner.class.getCanonicalName());

    //default the serdes to our journal entry types.
    kstreamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        JournalKeySerde.class.getCanonicalName());
    kstreamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        JournalValueSerde.class.getCanonicalName());

    return kstreamProps;
  }

  synchronized void load(KJournalImpl kjournal, KafkaJournalLoaderCallback callback) {
    if (streams != null && journalState.isRunningState()) {

      try {
        //todo: make this timeout configurable
        kjournal.loader().onLoadComplete().get(60L, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      StoreQueryParameters<ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry>>
          storeQueryParameters = StoreQueryParameters
          .fromNameAndType(
              kjournal.storeName(),
              QueryableStoreTypes.<JournalEntryKey, JournalEntry>keyValueStore())
          .enableStaleStores();

      StoreQueryParameters<ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry>>
          txStoreQueryParameters = StoreQueryParameters
          .fromNameAndType(
              kjournal.txStoreName(),
              QueryableStoreTypes.<JournalEntryKey, JournalEntry>keyValueStore())
          .enableStaleStores();

      //grab the state store now that we are loaded
      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> store =
          streams.store(storeQueryParameters);

      ReadOnlyKeyValueStore<JournalEntryKey, JournalEntry> txStore =
          streams.store(txStoreQueryParameters);

      //finish the callback
      kjournal.loader().executeLoadCallback(store, txStore, callback);

    } else {
      throw new IllegalStateException(
          "KafkaJournalProcessor must be started before being loaded. Current state is: "
              + journalState);
    }
  }

  @Override
  public void onChange(State newState, State oldState) {
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
        if (loadComplete) {
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

    return JournalTopology.createTopology(journals.values(), epochCoordinator, topoProps);
  }

  private void updateLoadProgress(JournalEntryKey key, JournalEntry entry) {

  }

  private void addJournalTopology(KJournalImpl kjournal, StreamsBuilder streamsBuilder) {
    KeyValueBytesStoreSupplier supplier =
        LoadingStore.wrap(Stores.persistentKeyValueStore(kjournal.storeName()));

    KStream<JournalEntryKey, JournalEntry> journalStream = streamsBuilder
        .table(kjournal.walTopic(),
            Consumed
                .<JournalEntryKey, JournalEntry>as(kjournal.prefix("journalSourceTable"))
                .withOffsetResetPolicy(AutoOffsetReset.EARLIEST),
            Materialized.<JournalEntryKey, JournalEntry>as(supplier).withCachingDisabled())
        .toStream(Named.as(kjournal.prefix("journalSourceTableToStream")))
        .peek(this::updateLoadProgress)
        .filter(
            (k, v) -> v != null, Named.as(kjournal.prefix("removeTombstones")))
        .filter(
            (k, v) -> !v.hasAnnotationReference() && !v.hasTransactionReference(),
            Named.as(kjournal.prefix("removeAggregates")));

    KStream<JournalEntryKey, JournalEntry> remaining = StreamsSupport
        .branchStream(journalStream, streamsBuilder)

        .when((k, v) -> v.hasAppendedRecord())
        .branchTo(st -> this.handleRecordsAndAnnotations(kjournal, streamsBuilder, st))

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
        () -> new TransactionProcessor(kjournal.walTopic(), kjournal.storeName()),
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
        .branchTo(st -> this.handleAddRecord(kjournal, streamsBuilder, st))

        .done();

    handleDeleteAnnotateRecord(kjournal, streamsBuilder, deleteAnnotatStream);
  }

  /**
   * Override this method to provide functionality around ADD_RECORD processing.
   */
  protected void handleAddRecord(
      KJournalImpl kjournal,
      StreamsBuilder sb,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    //this is were messages will be published out to kafka topics
    //Leave empty for now
  }

  protected void handleDeleteAnnotateRecord(
      KJournalImpl kjournal, StreamsBuilder sb,
      KStream<JournalEntryKey, JournalEntry> recordStream) {

    KStream<JournalEntryKey, JournalEntry> stream = recordStream.flatTransform(
        () -> new MainRecordProcessor(kjournal.walTopic(), kjournal.storeName()),
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

    entryStream.to(kjournal.walTopic());
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

    TopicSpec journalWalTopic();

    TopicSpec journalTableTopic();

    boolean performRouting();

    class Builder extends KafkaJournalProcessor_JournalSpec_Builder {

      public Builder() {
        this.performRouting(false);
      }
    }
  }

  @FreeBuilder
  public interface TopicSpec {

    String name();

    int partitions();

    int replication();

    Map<String, String> configs();

    class Builder extends KafkaJournalProcessor_TopicSpec_Builder {

    }
  }

  static class KJournalImpl implements KJournal {

    final JournalSpec spec;
    final KafkaJournalProcessor processor;
    final String storeName;
    final KafkaJournalLoader loader;

    KJournalImpl(
        JournalSpec spec,
        KafkaJournalProcessor processor,
        EpochCoordinator epochCoordinator) {

      this.spec = spec;
      this.processor = processor;
      this.storeName = spec.journalTableTopic().name() + "-store";
      this.loader = new KafkaJournalLoader(
          spec.journalName(), spec.journalWalTopic().partitions(), epochCoordinator);
    }

    @Override
    public KafkaJournalLoader loader() {
      return loader;
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
    public String walTopic() {
      return spec.journalWalTopic().name();
    }

    @Override
    public String tableTopic() {
      return spec.journalTableTopic().name();
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
