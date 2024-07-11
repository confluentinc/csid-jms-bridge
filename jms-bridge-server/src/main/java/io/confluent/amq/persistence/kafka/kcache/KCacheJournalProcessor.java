package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.JournalSpec;
import io.confluent.amq.persistence.kafka.journal.KJournal;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import io.confluent.amq.persistence.kafka.journal.impl.EpochCoordinator;
import io.confluent.amq.persistence.kafka.journal.impl.KafkaJournalProcessor;
import io.confluent.amq.util.Retry;
import org.apache.kafka.clients.admin.TopicDescription;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.amq.persistence.kafka.journal.KJournalState.CREATED;
import static io.confluent.amq.persistence.kafka.journal.KJournalState.STARTED;

public class KCacheJournalProcessor {
    private static final StructuredLogger SLOG = StructuredLogger
            .with(b -> b.loggerClass(KafkaJournalProcessor.class));

    private final String bridgeId;
    private final BridgeClientId bridgeClientId;
    private final String applicationId;
    private final Map<String, String> streamsConfig;
    private final KafkaIO kafkaIO;

    private final List<JournalSpec> journalSpecs;
    private final Map<String, JournalCache> journals;
    private final Duration loadTimeout;

    private volatile KJournalState journalState;
    private volatile boolean loadComplete = false;

    public KCacheJournalProcessor(
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


    public synchronized void start() {
        if (journalState.validTransition(STARTED)) {

            Retry retry = new Retry(5, Duration.ofSeconds(30), Duration.ofSeconds(1));
            retry.retry(
                    () -> ensureTopics(journalSpecs),
                    (j, err) -> j == null || j.isEmpty());

            initializeJournals();
            setupLoadingStateTransition();

        } else {
            SLOG.warn(b -> b
                    .event("StartJournal")
                    .markFailure()
                    .putTokens("currentState", journalState)
                    .putTokens("nextState", KJournalState.STARTED)
                    .message("Invalid state transition"));
        }
    }

    public List<JournalCache> getJournals() {
        return new ArrayList<>(journals.values());
    }

    public JournalCache getJournal(String name) {
        return journals.get(name);
    }

    private void initializeJournals() {
        //TODO: start the journals
    }

    public synchronized void stop() {
        if (journalState.validTransition(KJournalState.STOPPED)) {
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

    private void setupLoadingStateTransition() {

        CompletableFuture[] loadingfutures = new CompletableFuture[journalSpecs.size()];
        int idx = 0;
        for (JournalCache j : journals.values()) {
            loadingfutures[idx] = j.onLoadComplete();
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
        });


    }
}
