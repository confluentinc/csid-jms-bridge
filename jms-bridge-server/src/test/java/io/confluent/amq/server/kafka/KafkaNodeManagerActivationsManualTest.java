package io.confluent.amq.server.kafka;

import io.confluent.amq.config.HaConfig;
import io.confluent.amq.server.kafka.nodemanager.ClusterStates;
import io.confluent.amq.server.kafka.nodemanager.KafkaNodeManagerV2;
import io.confluent.amq.server.kafka.nodemanager.NodeLocks;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.amq.server.kafka.KafkaNodeManagerActivationsTest.GROUP_ID;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Slf4j
public class KafkaNodeManagerActivationsManualTest {


    /**
     * Manually executed test - to be used for testing failover and failback scenarios with abrupt live node crashes in conjunction with {@link KafkaNodeManagerActivationsTest#testLiveToBackupFailoverAndFailbackWithAbruptLiveCrashes}
     * Make sure to update bootsrap server config below in HaConfig to match Kafka endpoint exposed by Kafka Container that testLiveToBackupFailoverAndFailbackWithAbruptLiveCrashes stands up.
     */
    @Test
    @Disabled
    public void testFailoverAndFailbackWithAbruptLiveCrashing() throws Exception {
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = new HaConfig.Builder()
                .groupId(GROUP_ID + "_manual_test")
                .putAllConsumerConfig(
                        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "UPDATE_ME!!!")) //update bootstrap server to match kafka port exposed by backup test
                .build();
        KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);

        KafkaNodeManagerActivationsTest.LiveActivation liveActivation = new KafkaNodeManagerActivationsTest.LiveActivation(liveNodeManager, true, true);
        new Thread(liveActivation::start, "Live Activation Thread").start();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(100);
        //Dummy assert just to monitor activation state / distributed lock state - live node should be crashed by killing the process manually as per testLiveToBackupFailoverAndFailbackWithAbruptLiveCrashes
        await().atMost(Duration.ofSeconds(3600)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            log.info("Live state: activation state: {},current lock: {}, cluster state: {}", liveActivation.state, liveNodeManager.getCurrentLock(), liveNodeManager.getCurrentState());
            assertThat(liveNodeManager.isAlive()).isFalse();
        });
    }


}