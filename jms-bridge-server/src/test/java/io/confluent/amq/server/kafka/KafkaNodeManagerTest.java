package io.confluent.amq.server.kafka;

import io.confluent.amq.config.HaConfig;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction;
import org.apache.activemq.artemis.tests.integration.cluster.NodeManagerTest;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Assert;
import org.junit.ClassRule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaNodeManagerTest extends NodeManagerTest {
    @ClassRule
    public static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
                    .withNetworkAliases("kafka")
                    .withNetwork(Network.newNetwork())
                    .waitingFor(Wait.forListeningPort());
    static final String GROUP_ID = "knodeManagerTest_ha_groupId";
    public HaConfig haConfig = new HaConfig.Builder()
            .groupId(GROUP_ID)
            .putAllConsumerConfig(
                    Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))
            .build();

    @Override
    public void performWork(NodeManagerAction... actions) throws Exception {
        List<NodeManagerTest.NodeRunner> nodeRunners = new ArrayList<>();
        final ThreadFactory daemonThreadFactory = t -> {
            final Thread th = new Thread(t);
            th.setDaemon(true);
            return th;
        };
        Thread[] threads = new Thread[actions.length];
        List<ExecutorService> executors = new ArrayList<>(actions.length);
        List<NodeManager> nodeManagers = new ArrayList<>(actions.length * 2);
        AtomicBoolean failedRenew = new AtomicBoolean(false);
        for (NodeManagerAction action : actions) {
            final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory);
            final ExecutorService executor = Executors.newFixedThreadPool(2, daemonThreadFactory);

            KafkaNodeManager nodeManager = new KafkaNodeManager(
                    haConfig,
                    new UUID(java.util.UUID.randomUUID()),
                    false);
            nodeManager.start();
            NodeManagerTest.NodeRunner nodeRunner = new NodeManagerTest.NodeRunner(nodeManager, action);
            nodeRunners.add(nodeRunner);
            nodeManagers.add(nodeManager);
            executors.add(scheduledExecutorService);
            executors.add(executor);
        }
        for (int i = 0, nodeRunnersSize = nodeRunners.size(); i < nodeRunnersSize; i++) {
            NodeManagerTest.NodeRunner nodeRunner = nodeRunners.get(i);
            threads[i] = new Thread(nodeRunner);
            threads[i].start();
        }
        boolean isDebug = isDebug();
        // forcibly stop node managers
        nodeManagers.forEach(nodeManager -> {
            try {
                nodeManager.stop();
            } catch (Exception e) {
                // won't prevent the test to complete
                e.printStackTrace();
            }
        });
        for (Thread thread : threads) {
            try {
                if (isDebug) {
                    thread.join();
                } else {
                    thread.join(60_000);
                }
            } catch (InterruptedException e) {
                //
            }
            if (thread.isAlive()) {
                thread.interrupt();
                Assert.fail("thread still running");
            }
        }


        // stop executors
        executors.forEach(ExecutorService::shutdownNow);

        for (NodeManagerTest.NodeRunner nodeRunner : nodeRunners) {
            if (nodeRunner.e != null) {
                nodeRunner.e.printStackTrace();
                Assert.fail(nodeRunner.e.getMessage());
            }
        }
        Assert.assertFalse("Some of the lease locks has failed to renew the locks", failedRenew.get());
    }
}