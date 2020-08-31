package io.confluent.amq.server.kafka;

import io.confluent.amq.logging.LogFormat;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Kafka implementation of a {@link SharedStateManager}.
 */
@SuppressWarnings("SynchronizeOnNonFinalField")
public class KafkaSharedStateManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNodeManager.class);
    private static final LogFormat LOG_FORMAT = LogFormat.forSubject("KafkaIO");
    private AdminClient adminClient;

    //implementation of node managment in Kafka
    // Things that must be implemented:
    // ToDo: Create Topic if none exists - Done
    // ToDo: 1 consumer per Artemis broker
    // ToDo: Each consumer substribes to the consumer and identifies what is there assigned partition














    public boolean createTopicIfNotExists(String name, int partitions, int replication,
                                          Map<String, String> options) {
        // ToDo: Get the
        if (listTopics().contains(name)) {
            return false;
        } else {
            createTopic(name, partitions, replication, options);
            return true;
        }
    }

    public void createTopic(String name, int partitions, int replication,
                            Map<String, String> options) {

        LOGGER.info(LOG_FORMAT.build(b -> b
                .event("CreateTopic")
                .putTokens("topic", name)
                .putTokens("partitions", partitions)
                .putTokens("replication", replication)));

        NewTopic topic = new NewTopic(name, partitions, (short) replication);
        topic.configs(options);
        try {
            adminClient.createTopics(Collections.singletonList(topic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public Set<String> listTopics() {
        try {
            return adminClient.listTopics().names().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}