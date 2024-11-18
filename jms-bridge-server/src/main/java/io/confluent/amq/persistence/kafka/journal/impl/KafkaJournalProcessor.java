/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.persistence.kafka.LoadInitializer;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.inferred.freebuilder.FreeBuilder;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import io.confluent.amq.util.Retry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.amq.persistence.kafka.journal.KJournalState.*;

/**
 * The journal processor executes outside of the normal flow of Artemis's runtime. It is primarily responsible for
 * processing the WAL topic, in the background, to maintain the global state store.
 *
 * There is one place that the kafka streams application does interact directly with Artemis and that is during startup.
 * Part of startup includes the loading step which requires that most up to date state is retreived from the state
 * store.
 *
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

  private final String bridgeId;
  private final BridgeClientId bridgeClientId;
  private final String applicationId;
  private final Map<String, String> streamsConfig;
  private final KafkaIO kafkaIO;
  private final List<JournalSpec> journalSpecs;
  private final Map<String, KJournalImpl> journals;
  private final Duration loadTimeout;

  private volatile KJournalState journalState;
  private volatile KafkaStreams streams;
  private volatile boolean loadComplete = false;
  private boolean firstLoad = true;

  private CountDownLatch loadCompleteLatch = new CountDownLatch(1);

  private final LoadInitializer loadInitializer = new LoadInitializer();

  public KafkaJournalProcessor(
      String bridgeId,
      List<JournalSpec> journalSpecs,
      BridgeClientId bridgeClientId,
      String applicationId,
      Duration loadTimeOut,
      Map<String, String> streamsConfig,
      KafkaIO kafkaIO) {

    this.bridgeId = bridgeId;
    this.bridgeClientId = bridgeClientId;
    this.applicationId = applicationId;
    this.loadTimeout = loadTimeOut;
    this.streamsConfig = streamsConfig;
    this.kafkaIO = kafkaIO;

    this.journalState = CREATED;
    this.journalSpecs = journalSpecs;
    journals = new HashMap<>();
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

  public void loadComplete(String journalName) {
    SLOG.debug(b -> b
            .event("LoadJournal")
            .putTokens("phase", "LoadComplete")
            .putTokens("journalName", journalName)
    );

    loadCompleteLatch.countDown();
  }
  public synchronized void start() {
    if (journalState.validTransition(STARTED)) {
      Retry retry = new Retry(5, Duration.ofSeconds(30), Duration.ofSeconds(1));
      retry.retry(
          () -> ensureTopics(journalSpecs),
          (j, err) -> j == null || j.isEmpty());
      // Track if its first time loading is done (i.e. first time start() is called or its a subsequent reload due to
      // switching between passive / active modes multiple times.
      // Reset the state of journals / loading as new KafkaJournalProcessor is not constructed between Backup giving up control
      // on failback and then getting control on failover again - so need to make sure that any load state is reset back to allow
      // clean journal reload.
      loadComplete = false;
      if(firstLoad) {
        initializeJournals();
        this.firstLoad = false;
      } else {
        journals.values().forEach(KJournalImpl::reset);
      }
      Topology topology = createTopology();
      SLOG.debug(b -> b
          .event("TopologyDescription")
          .message(topology.describe().toString()));

      streams = new KafkaStreams(topology, effectiveStreamProperties());
      streams.setStateListener(this);
      streams.start();

      //may have already gone to assigning
      if (journalState.validTransition(KJournalState.STARTED)) {
        transitionState(KJournalState.STARTED);
      }
    } else {
      SLOG.warn(b -> b
          .event("StartJournal")
          .markFailure()
          .putTokens("currentState", journalState)
          .putTokens("nextState", KJournalState.STARTED)
          .message("Invalid state transition"));
    }
  }

  public synchronized void stop() {
    if (journalState.validTransition(KJournalState.STOPPING)) {
      loadComplete = false;

      SLOG.info(b -> b
              .event("StopJournal")
              .markSuccess()
              .putTokens("currentState", journalState)
              .putTokens("nextState", KJournalState.STOPPING));
      journalState = KJournalState.STOPPING;

      if (streams != null) {
        streams.close();
      }

    } else {
      SLOG.error(b -> b
          .event("StopJournal")
          .markFailure()
          .putTokens("currentState", journalState)
          .putTokens("nextState", KJournalState.STOPPING)
          .message("Invalid state transition"));
    }
  }


  public Properties effectiveStreamProperties() {
    Properties kstreamProps = new Properties();
    kstreamProps.putAll(streamsConfig);
    kstreamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    kstreamProps.put(StreamsConfig.CLIENT_ID_CONFIG, bridgeClientId.clientId("jms-bridge"));

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

  /**
   * This load is called from the KafkaJournal which was invoked during startup of Artemis via the
   * JournalStorageManager. In order to gather the current state we need access the global state store and the
   * transaction store. The transaction store is needed because there may be unprocessed, yet completed, transactions
   * in the log, it is also where JTA transactions live.
   *
   * The initial onLoadComplete() call blocks until the WAL topic has been completely processed and the global state
   * store is up to date. It uses a sentinel message to achieve this,
   *  see {@link io.confluent.amq.persistence.domain.proto.EpochEvent}. The sentinal message is produced by
   *  {@link LoadInitializer} prior to execution of journal loading.
   *
   */
  synchronized void load(KJournalImpl kjournal, KafkaJournalLoaderCallback callback) {
    if (streams != null && journalState.isRunningState()) {

      waitForStreamsRunningState();
      if(loadCompleteLatch.getCount() == 0) {
          loadCompleteLatch = new CountDownLatch(1);
      }

      loadInitializer.fireEpochs(kafkaIO, kjournal.walTopic());

      long startTime = System.currentTimeMillis();
      boolean loaded = false;
      while (!loaded){
        try {
          loaded = loadCompleteLatch.await(200L,TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        if (System.currentTimeMillis() > startTime + loadTimeout.toMillis()) {
          throw new RuntimeException("Timed out waiting for journal load completion.");
        }
      }

      kjournal.setLoaded(true);

      boolean allJournalsLoaded = journals.values().stream().allMatch(KJournalImpl::isLoaded);

      if(allJournalsLoaded){
          transitionToRunning();
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

  private void waitForStreamsRunningState() {
      //wait for the streams to be running
      while (streams.state() != State.RUNNING && (streams.state() == State.REBALANCING || streams.state() == State.CREATED)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted during journal load initiation while waiting for Streams to be in RUNNING state",e);
        }
      }
  }

  @Override
  public void onChange(State newState, State oldState) {
    transitionState(journalStateFromStreamsState(oldState, newState));
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

  protected void initializeJournals() {
    journalSpecs.stream()
        .map(js -> new KJournalImpl(js, this, loadInitializer))
        .forEach(kj -> journals.put(kj.name(), kj));
  }

  protected Topology createTopology() {

    Properties topoProps = new Properties();
    topoProps.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    JournalTopology.TopologySpec spec = new JournalTopology.TopologySpecBuilder()
        .addAllJournals(journals.values())
        .topologyProps(topoProps)
        .bridgeId(bridgeId)
        .build();
    return JournalTopology.createTopology(spec);
  }


  private void transitionToRunning() {
      loadComplete = true;
      if (journalState == KJournalState.LOADING || journalState == ASSIGNING) {
        transitionState(KJournalState.RUNNING);
      }
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
        state = STOPPING;
        break;
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
    final LoadInitializer loadInitializer;

    @Getter
    @Setter
    boolean loaded = false;

    KJournalImpl(
        JournalSpec spec,
        KafkaJournalProcessor processor,
        LoadInitializer loadInitializer) {

      this.spec = spec;
      this.processor = processor;
      this.storeName = spec.journalTableTopic().name() + "-store";
      this.loadInitializer = loadInitializer;
      this.loader = new KafkaJournalLoader(
          spec.journalName(), spec.journalWalTopic().partitions(), loadInitializer, () -> processor.loadComplete(spec.journalName()));
    }

    @Override
    public KafkaJournalLoader loader() {
      return loader;
    }

    @Override
    public void stop() {
      processor.stop();
    }

    public void reset(){
      loader.reset();
      loadInitializer.resetEpochMarker();
      loaded=false;
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
