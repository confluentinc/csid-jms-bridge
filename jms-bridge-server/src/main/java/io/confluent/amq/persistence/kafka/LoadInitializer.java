package io.confluent.amq.persistence.kafka;

import com.google.protobuf.Message;
import io.confluent.amq.persistence.domain.proto.EpochEvent;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.kafka.journal.impl.EpochCoordinator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

/**
 * This is an alternative to using the {@link io.confluent.amq.persistence.kafka.journal.impl.EpochPuntcuator} which
 * does not work when multiple nodes are sharing stream tasks. In order for it to work the node coming up must take
 * control of all tasks in order for the puntcuator to fire for all journals.
 *
 * This class will publish to each partition of the journals WAL topic an
 * {@link io.confluent.amq.persistence.domain.proto.EpochEvent} which is a sentinal message used for verifying the
 * completeness of the journal state store.
 */
@Slf4j
public class LoadInitializer {

    //need wal topics
    //kafka producer

    public static void fireEpochs(EpochCoordinator epochCoordinator, KafkaIO kafkaIO, List<String> walTopics) {
        KafkaProducer<Message, Message> producer = kafkaIO.getInternalProducer();
        for (String walTopic: walTopics) {
            List<PartitionInfo> partitionList = producer.partitionsFor(walTopic);
            for (PartitionInfo pInfo: partitionList) {
                JournalEntry epochEntry = JournalEntry
                        .newBuilder()
                        .setEpochEvent(EpochEvent.newBuilder()
                                .setEpochId(epochCoordinator.getInitialEpoch())
                                .setEpochStage(EpochCoordinator.EPOCH_STAGE_START)
                                .setPartition(pInfo.partition()))
                        .build();

                ProducerRecord<Message, Message> pRecord = new ProducerRecord<>(
                        walTopic, pInfo.partition(), KafkaRecordUtils.epochKey(), epochEntry);

                try {
                    producer.send(pRecord).get();
                } catch (Exception e) {
                    log.error("Failed to publish epoch event during loading initialization", e);
                }
            }
        }
    }
}
