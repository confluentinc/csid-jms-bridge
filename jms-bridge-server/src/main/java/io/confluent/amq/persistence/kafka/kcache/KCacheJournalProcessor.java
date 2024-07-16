package io.confluent.amq.persistence.kafka.kcache;

import static io.confluent.amq.persistence.kafka.journal.KJournalState.CREATED;
import static io.confluent.amq.persistence.kafka.journal.KJournalState.STARTED;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.journal.JournalSpec;
import io.confluent.amq.persistence.kafka.journal.KJournalState;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KCacheJournalProcessor {
    private static final StructuredLogger SLOG = StructuredLogger
            .with(b -> b.loggerClass(KCacheJournalProcessor.class));

    private final String bridgeId;
    private final BridgeClientId bridgeClientId;
    private final KafkaIO kafkaIO;

    private final List<JournalSpec> journalSpecs;
    private final Map<String, JournalCache> journals;
    private final Duration loadTimeout;

    private CompletableFuture<Void> onLoadCompleted;
    private volatile KJournalState journalState;
    private volatile boolean loadComplete = false;

    public KCacheJournalProcessor(
            String bridgeId,
            List<JournalSpec> journalSpecs,
            BridgeClientId bridgeClientId,
            Duration loadTimeOut,
            KafkaIO kafkaIO) {
        this.bridgeId = bridgeId;
        this.bridgeClientId = bridgeClientId;
        this.kafkaIO = kafkaIO;
        this.loadTimeout = loadTimeOut;

        this.journalState = CREATED;
        this.journalSpecs = journalSpecs;
        journals = new HashMap<>();
    }


    public synchronized void start() {
        if (journalState.validTransition(STARTED)) {
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
        for (JournalSpec js : journalSpecs) {
            JournalCache jCache = new JournalCache(bridgeId, js.journalName(), bridgeClientId, js.kcacheConfig());
            jCache.start();
            journals.put(js.journalName(), jCache);
        }
    }

    public synchronized void stop() throws IOException {

        if (journalState.validTransition(KJournalState.STOPPED)) {
            for (JournalCache jc : journals.values()) {
                jc.stop();
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

    private void setupLoadingStateTransition() {

        CompletableFuture[] loadingfutures = new CompletableFuture[journalSpecs.size()];
        int idx = 0;
        for (JournalCache j : journals.values()) {
            loadingfutures[idx] = j.onLoadComplete();
            idx++;
        }

        this.onLoadCompleted = CompletableFuture.allOf(loadingfutures).whenComplete((nil, t) -> {
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
