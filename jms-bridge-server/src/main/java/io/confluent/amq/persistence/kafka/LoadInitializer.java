package io.confluent.amq.persistence.kafka;

import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.EpochEvent;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

/**
 * This class will publish to each partition of the journals WAL topic an
 * {@link io.confluent.amq.persistence.domain.proto.EpochEvent} which is a sentinel message used for verifying the
 * completeness of the journal state store.
 */
@Slf4j
public class LoadInitializer {
    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(LoadInitializer.class));

    public static final String EPOCH_STAGE_START = "START";
    public static final String EPOCH_STAGE_READY = "READY";

    private long epochMarker = -1;

    public void fireEpochs(KafkaIO kafkaIO, String walTopic) {
        KafkaProducer<JournalEntryKey, JournalEntry> producer = kafkaIO.getInternalProducer();

        epochMarker = System.currentTimeMillis();
        SLOG.debug(b -> b
                .name("LoadInitializer")
                .event("FireEpochs")
                .markStarted()
                .putTokens("epochMarker", epochMarker));
        List<PartitionInfo> partitionList = producer.partitionsFor(walTopic);
        for (PartitionInfo pInfo : partitionList) {
            JournalEntry epochEntry = JournalEntry
                    .newBuilder()
                    .setEpochEvent(EpochEvent.newBuilder()
                            .setEpochId(epochMarker)
                            .setEpochStage(LoadInitializer.EPOCH_STAGE_START)
                            .setPartition(pInfo.partition()))
                    .build();

            ProducerRecord<JournalEntryKey, JournalEntry> pRecord = new ProducerRecord<>(
                    walTopic, pInfo.partition(), KafkaRecordUtils.epochKey(), epochEntry);

            try {
                producer.send(pRecord).get();
            } catch (Exception e) {
                log.error("Failed to publish epoch event during loading initialization", e);
            }
        }

        SLOG.debug(b -> b
                .name("LoadInitializer")
                .event("FireEpochs")
                .markCompleted());
    }

    public long getEpochMarker() {
        return epochMarker;
    }

    public void resetEpochMarker() {
        epochMarker = -1;
    }
}
