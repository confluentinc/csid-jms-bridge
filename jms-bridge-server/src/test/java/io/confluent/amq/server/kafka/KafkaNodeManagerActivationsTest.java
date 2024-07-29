package io.confluent.amq.server.kafka;

import io.confluent.amq.config.HaConfig;
import io.confluent.amq.server.kafka.nodemanager.ClusterStates;
import io.confluent.amq.server.kafka.nodemanager.KafkaNodeManagerV2;
import io.confluent.amq.server.kafka.nodemanager.NodeLocks;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Slf4j
public class KafkaNodeManagerActivationsTest {
    @ClassRule
    public static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
                    .withNetworkAliases("kafka")
                    .withNetwork(Network.newNetwork())
                    .waitingFor(Wait.forListeningPort());
    public static final String GROUP_ID = "knodeManagerActivationTest_ha_groupId";

    @Test
    public void testFailback_StartingLiveFirstBackupSecond() throws Exception {
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        UUID backupUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("85b4b78b-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = haConfig();
        KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);

        LiveActivation liveActivation = new LiveActivation(liveNodeManager, true, true);
        new Thread(() -> liveActivation.start(), "Live activation").start();


        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });


        KafkaNodeManagerV2 backupNodeManager = new KafkaNodeManagerV2(
                haConfig,
                backupUUID,
                false,
                false);
        BackupActivation backupActivation = new BackupActivation(backupNodeManager, true, true, true, true);
        new Thread(() -> backupActivation.start(), "Backup activation").start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.BACKUP);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });
        Thread.sleep(100);
        log.debug("Time to stop live");
        liveActivation.stop(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveNodeManager.isAlive()).isFalse();
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.NONE);
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STOPPED);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(2_000);

        log.debug("Time to start live again");
        KafkaNodeManagerV2 liveNodeManager2 = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);

        LiveActivation liveActivation2 = new LiveActivation(liveNodeManager2, true, true);
        liveActivation2.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation2.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager2.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager2.isAlive()).isTrue();
            assertThat(liveNodeManager2.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.getCurrentLock()).isNotEqualTo(NodeLocks.LIVE);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock().equals(NodeLocks.BACKUP));
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        //give some time for logs to be printed and for state negotiations to be definitely complete.
        Thread.sleep(10_000);
        assertThat(backupActivation.haMaster).isFalse();
        assertThat(backupActivation.failbackStartedByChecker).isTrue();

    }

    @Test
    public void testFailback_StartingSimultaneouslyLiveAndBackup() throws Exception {
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        UUID backupUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("85b4b78b-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = haConfig();

        KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);
        KafkaNodeManagerV2 backupNodeManager = new KafkaNodeManagerV2(
                haConfig,
                backupUUID,
                false,
                false);
        LiveActivation liveActivation = new LiveActivation(liveNodeManager, true, true);
        new Thread(() -> liveActivation.start(), "Live activation").start();
        BackupActivation backupActivation = new BackupActivation(backupNodeManager, true, true, true, true);
        new Thread(() -> backupActivation.start(), "Backup activation").start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);

            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.BACKUP);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });
        Thread.sleep(100);
        log.debug("Time to stop live");
        liveActivation.stop(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveNodeManager.isAlive()).isFalse();
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.NONE);
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STOPPED);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(2_000);

        log.debug("Time to start live again");
        KafkaNodeManagerV2 liveNodeManager2 = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);

        LiveActivation liveActivation2 = new LiveActivation(liveNodeManager2, true, true);
        liveActivation2.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation2.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager2.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager2.isAlive()).isTrue();
            assertThat(liveNodeManager2.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.getCurrentLock()).isNotEqualTo(NodeLocks.LIVE);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock().equals(NodeLocks.BACKUP));
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        //give some time for logs to be printed and for state negotiations to be definitely complete.
        Thread.sleep(10_000);
        assertThat(backupActivation.haMaster).isFalse();
        assertThat(backupActivation.failbackStartedByChecker).isTrue();

    }

    //@Test
    public void testFailback_StartingSimultaneouslyLiveAndBackupKillingLiveThread() throws Exception {
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        UUID backupUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("85b4b78b-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = haConfig();


        KafkaNodeManagerV2 backupNodeManager = new KafkaNodeManagerV2(
                haConfig,
                backupUUID,
                false,
                false);
        final KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);
        final LiveActivation liveActivation = new LiveActivation(liveNodeManager, true, true);

        ExecutorService liveThread = Executors.newSingleThreadExecutor();
        liveThread.submit(liveActivation::start);
        BackupActivation backupActivation = new BackupActivation(backupNodeManager, true, true, true, true);
        new Thread(() -> backupActivation.start(), "Backup activation").start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);

            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.BACKUP);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });
        Thread.sleep(100);
        log.debug("Time to stop live");
        liveThread.shutdownNow();
        liveNodeManager.stop();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveNodeManager.isAlive()).isFalse();
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.NONE);
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STOPPED);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(2_000);

        log.debug("Time to start live again");
        KafkaNodeManagerV2 liveNodeManager2 = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);

        LiveActivation liveActivation2 = new LiveActivation(liveNodeManager2, true, true);
        liveActivation2.start();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation2.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager2.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager2.isAlive()).isTrue();
            assertThat(liveNodeManager2.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.getCurrentLock()).isNotEqualTo(NodeLocks.LIVE);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock().equals(NodeLocks.BACKUP));
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        //give some time for logs to be printed and for state negotiations to be definitely complete.
        Thread.sleep(10_000);
        assertThat(backupActivation.haMaster).isFalse();
        assertThat(backupActivation.failbackStartedByChecker).isTrue();

    }

    //@Test
    public void testFailover_StartingBackupFirstLiveSecond() throws Exception {
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        UUID backupUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("85b4b78b-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = haConfig();

        KafkaNodeManagerV2 backupNodeManager = new KafkaNodeManagerV2(
                haConfig,
                backupUUID,
                false,
                false);

        BackupActivation backupActivation = new BackupActivation(backupNodeManager, true, true, true, true);
        new Thread(() -> backupActivation.start(), "Backup Activation Thread").start();
        Thread.sleep(2_000);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupActivation.state == ActiveMQServer.SERVER_STATE.STARTED);
        });
        Thread.sleep(100);

        KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);
        LiveActivation liveActivation = new LiveActivation(liveNodeManager, true, true);
        new Thread(() -> liveActivation.start(), "Live Activation Thread").start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
            assertThat(backupNodeManager.getCurrentLock()).isNotEqualTo(NodeLocks.LIVE);
        });
        Thread.sleep(100);
        liveActivation.stop(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveNodeManager.isAlive()).isFalse();
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.NONE);
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STOPPED);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(10_000);
    }

    @Test
    public void testLiveOnly() throws Exception {
        ActiveMQUnBlockedException e;
        UUID liveUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("8cdb294c-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig = haConfig();

        KafkaNodeManagerV2 liveNodeManager = new KafkaNodeManagerV2(
                haConfig,
                liveUUID,
                false,
                true);
        LiveActivation liveActivation = new LiveActivation(liveNodeManager, true, true);
        new Thread(() -> liveActivation.start(), "Live Activation Thread").start();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(liveNodeManager.isAlive()).isTrue();
            assertThat(liveNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });
        Thread.sleep(100);
        liveActivation.stop(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(liveActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STOPPED);
            assertThat(liveNodeManager.getCurrentLock()).isEqualTo(NodeLocks.NONE);
            assertThat(liveNodeManager.isAlive()).isFalse();
        });
    }

    /**
     * Disabled in automated runs - test for sequencing activations with {@link KafkaNodeManagerActivationsManualTest#testFailoverAndFailbackWithAbruptLiveCrashing}.
     * Should be executed manually with the live node ran in that other test alongside, killing the live node process and restarting at specific points of the test.
     * After this test is started - need to update Kafka exposed port in the testFailoverAndFailbackWithAbruptLiveCrashing code to match the random assigned one.
     */
    //@Disabled
    //@Test
    public void testLiveToBackupFailoverAndFailbackWithAbruptLiveCrashes() {
        UUID backupUUID = new UUID(UUID.TYPE_NAME_BASED, UUID.stringToBytes("85b4b78b-2355-11ef-97fc-bed413035aef"));
        HaConfig haConfig =new HaConfig.Builder()
                .groupId(GROUP_ID + "_manual_test")
                .putAllConsumerConfig(
                        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))
                .build();

        KafkaNodeManagerV2 backupNodeManager = new KafkaNodeManagerV2(
                haConfig,
                backupUUID,
                false,
                false);

        BackupActivation backupActivation = new BackupActivation(backupNodeManager, true, true, true, true);
        new Thread(() -> backupActivation.start(), "Backup Activation Thread").start();

        //Start live node by executing {@link KafkaNodeManagerActivationsManualTest#testFailoverAndFailbackWithAbruptLiveCrashing}
        await().atMost(Duration.ofSeconds(3600)).untilAsserted(() -> {
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.BACKUP);
            assertThat(backupNodeManager.isAlive()).isTrue();
            assertThat(backupNodeManager.getCurrentState()).isEqualTo(ClusterStates.LIVE);
        });

        //Kill live node by terminating the {@link KafkaNodeManagerActivationsManualTest#testFailoverAndFailbackWithAbruptLiveCrashing} process
        await().atMost(Duration.ofSeconds(3600)).untilAsserted(() -> {
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });

        //Start live node by executing {@link KafkaNodeManagerActivationsManualTest#testFailoverAndFailbackWithAbruptLiveCrashing} - asserts below verify failback happens
        await().atMost(Duration.ofSeconds(3600)).untilAsserted(() -> {
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.BACKUP);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });

        //Kill live node again and check that failover triggers second time as well
        await().atMost(Duration.ofSeconds(3600)).untilAsserted(() -> {
            assertThat(backupActivation.state).isEqualTo(ActiveMQServer.SERVER_STATE.STARTED);
            assertThat(backupNodeManager.getCurrentLock()).isEqualTo(NodeLocks.LIVE);
            assertThat(backupNodeManager.isAlive()).isTrue();
        });
    }

    private HaConfig haConfig() {
        return new HaConfig.Builder()
                .groupId(GROUP_ID + "_" + RandomStringUtils.randomAlphanumeric(10))
                .putAllConsumerConfig(
                        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))
                .build();
    }

    @RequiredArgsConstructor
    class BackupActivation {
        ActiveMQServer.SERVER_STATE state = ActiveMQServer.SERVER_STATE.STOPPED;
        private final NodeManager nodeManager;

        private final boolean isWaitForActivation;
        private CountDownLatch activationLatch;

        private AtomicBoolean restarting = new AtomicBoolean(false);

        ActivateCallback nodeManagerActivateCallback;

        private final boolean isFailBackEnabled;

        private final boolean isFailoverOnShutdown;
        private volatile boolean cancelFailBackChecker;

        private final boolean isRestartBackup;

        @Getter
        @Setter
        private boolean haMaster = false;

        public boolean simulatedBackupListenerWaitingForBackup = true;

        public boolean failbackCheckerStarted = false;
        public boolean backupLockAcquired = false;
        public boolean liveLockAcquired = false;
        public boolean backupLockReleased = false;
        public boolean failbackCheckExecuting = false;
        public boolean failbackStartedByChecker = false;

        public NodeManager getNodeManager() {
            return nodeManager;
        }

        Thread activationThread;

        private final Object failbackCheckerGuard = new Object();

        @SneakyThrows
        public void start() {
            if (state != ActiveMQServer.SERVER_STATE.STOPPED) {
                log.warn("Server already started!");
                return;
            }
            state = ActiveMQServer.SERVER_STATE.STARTING;

            activationLatch = new CountDownLatch(1);

            nodeManager.start();
            Runnable activation = () -> {
                try {
                    nodeManager.startBackup();
                    backupLockAcquired = true;
                    synchronized (this) {
                        if (state == ActiveMQServerImpl.SERVER_STATE.STOPPED
                                || state == ActiveMQServerImpl.SERVER_STATE.STOPPING) {
                            return;
                        }
                        state = ActiveMQServerImpl.SERVER_STATE.STARTED;

                        nodeManager.awaitLiveNode();
                        liveLockAcquired = true;
                        log.info("Fail over to this node started");

                        setHaMaster(true);
                        initialisePart2();

                        completeActivation();
                        log.info("Backup Server is Live");

                        nodeManager.releaseBackup();
                        backupLockReleased = true;
                        if (isFailBackEnabled && ActiveMQServerImpl.SERVER_STATE.STOPPING != state && ActiveMQServerImpl.SERVER_STATE.STOPPED != state) {
                            startFailbackChecker();
                            failbackCheckerStarted = true;
                        }

                    }
                } catch (Exception e) {
                    log.error("Error starting server", e);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            };
            activationThread = new Thread(activation, "Backup Activation Thread");
            activationThread.start();

            activationLatch.await();
        }

        private void startFailbackChecker() {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new FailbackChecker(), 1000L, 1000L, TimeUnit.MILLISECONDS);
        }


        synchronized void initialisePart2() throws Exception {
            // Load the journal and populate queues, transactions and caches in memory

            if (state == ActiveMQServer.SERVER_STATE.STOPPED || state == ActiveMQServer.SERVER_STATE.STOPPING) {
                return;
            }
        }

        public void completeActivation() throws Exception {
            state = ActiveMQServerImpl.SERVER_STATE.STARTED;
            activationLatch.countDown();
        }

        @SneakyThrows
        public void stop(boolean isFailoverOnShutdown, boolean criticalError, boolean restarting) {
            ActivateCallback activateCallback = nodeManagerActivateCallback;
            if (activateCallback != null) {
                activateCallback.preDeActivate();
            }

            state = ActiveMQServer.SERVER_STATE.STOPPED;
            close(isFailoverOnShutdown, restarting);
        }

        public void interruptActivationThread(NodeManager nodeManagerInUse) throws InterruptedException {
            long timeout = 30000;

            long start = System.currentTimeMillis();

            while (activationThread.isAlive() && System.currentTimeMillis() - start < timeout) {
                if (nodeManagerInUse != null) {
                    nodeManagerInUse.interrupt();
                }

                activationThread.interrupt();

                activationThread.join(1000);

            }

            if (System.currentTimeMillis() - start >= timeout) {
                log.warn("Timed out waiting for activation to exit");
            }
        }

        public void close(boolean permanently, boolean restarting) throws Exception {
            if (!restarting) {
                synchronized (failbackCheckerGuard) {
                    cancelFailBackChecker = true;
                }
            }
            // To avoid a NPE cause by the stop
            NodeManager nodeManagerInUse = getNodeManager();

            //we need to check as the servers policy may have changed
            if (!isHaMaster()) {

                interruptActivationThread(nodeManagerInUse);

                if (nodeManagerInUse != null) {
                    //unregisterActiveLockListener(nodeManagerInUse);
                    nodeManagerInUse.stopBackup();
                }
            } else {

                if (nodeManagerInUse != null) {
                    //unregisterActiveLockListener(nodeManagerInUse);
                    // if we are now live, behave as live
                    // We need to delete the file too, otherwise the backup will failover when we shutdown or if the backup is
                    // started before the live
                    if (isFailoverOnShutdown || permanently) {
                        try {
                            nodeManagerInUse.crashLiveServer();
                        } catch (Throwable t) {
                            if (!permanently) {
                                throw t;
                            }
                            log.warn("Errored while closing activation: can be ignored because of permanent close", t);
                        }
                    } else {
                        nodeManagerInUse.pauseLiveServer();
                    }
                }
            }
        }

        private class FailbackChecker implements Runnable {

            @Override
            public void run() {
                try {
                    if (!restarting.get() && nodeManager.isAwaitingFailback() && simulatedBackupListenerWaitingForBackup) {
                        if (!restarting.compareAndSet(false, true)) {
                            return;
                        }
                        failbackCheckExecuting = true;
                        log.info("Await failback");
                        Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    log.info("{}::Stopping live node in favor of failback", this);
                                    failbackStartedByChecker = true;
                                    NodeManager nodeManager = getNodeManager();
                                    stop(true, false, true);

                                    // ensure that the server to which we are failing back actually starts fully before we restart
                                    nodeManager.start();
                                    try {
                                        nodeManager.awaitLiveStatus();
                                    } finally {
                                        nodeManager.stop();
                                    }

                                    synchronized (failbackCheckerGuard) {
                                        if (cancelFailBackChecker || !isRestartBackup)
                                            return;
                                        setHaMaster(false);
                                        log.info("{}::Starting backup node now after failback", this);
                                        start();

                                        //NodeManager.LockListener lockListener = activeLockListener;
                                        //if (lockListener != null) {
                                        //    activeMQServer.getNodeManager().registerLockListener(lockListener);
                                        //}
                                    }
                                } catch (Exception e) {
                                    log.warn("Unable to restart server, please kill and restart manually, ", e);
                                }
                            }
                        }, "Failback checker thread");
                        t.start();
                    }
                } catch (Exception e) {
                    log.warn("Unable to restart server, please kill and restart manually, ", e);
                }
            }
        }
    }

    @RequiredArgsConstructor
    static class LiveActivation {
        ActiveMQServer.SERVER_STATE state = ActiveMQServer.SERVER_STATE.STOPPED;
        private final NodeManager nodeManager;

        private final boolean isFailoverOnShutdown;

        private final boolean isWaitForActivation;
        private CountDownLatch activationLatch;

        ActivateCallback nodeManagerActivateCallback;

        public NodeManager getNodeManager() {
            return nodeManager;
        }

        @SneakyThrows
        public void start() {
            if (state != ActiveMQServer.SERVER_STATE.STOPPED) {
                log.warn("Server already started!");
                return;
            }
            state = ActiveMQServer.SERVER_STATE.STARTING;

            activationLatch = new CountDownLatch(1);

            nodeManager.start();
            Runnable activation = () -> {
                try {
                    if (nodeManager.isBackupLive()) {
                        /*
                         * looks like we've failed over at some point need to inform that we are the
                         * backup so when the current live goes down they failover to us
                         */
                        log.info("Detected that backup is live");

                        if (!isWaitForActivation)
                            state = ActiveMQServer.SERVER_STATE.STARTED;
                    }

                    nodeManagerActivateCallback = nodeManager.startLiveNode();

                    synchronized (this) {
                        if (state == ActiveMQServerImpl.SERVER_STATE.STOPPED
                                || state == ActiveMQServerImpl.SERVER_STATE.STOPPING) {
                            return;
                        }

                        initialisePart2();

                        completeActivation();

                        log.info("Server is Live");
                    }
                } catch (Exception e) {
                    log.error("Error starting server", e);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            };
            if (!isWaitForActivation) {
                activation.run();
            } else {
                new Thread(activation, "Live Activation Thread").start();
            }
            activationLatch.await();
        }

        @SneakyThrows
        public void stop(boolean permanently) {
            ActivateCallback activateCallback = nodeManagerActivateCallback;
            if (activateCallback != null) {
                activateCallback.preDeActivate();
            }

            state = ActiveMQServer.SERVER_STATE.STOPPED;

            if (nodeManager != null) {
                if (isFailoverOnShutdown || permanently) {
                    try {
                        nodeManager.crashLiveServer();
                    } catch (Throwable t) {
                        if (!permanently) {
                            throw t;
                        }
                        log.warn("Errored while closing activation: can be ignored because of permanent close", t);
                    }
                } else {
                    nodeManager.pauseLiveServer();
                }
            }
            nodeManager.stop();
        }

        synchronized void initialisePart2() throws Exception {
            // Load the journal and populate queues, transactions and caches in memory

            if (state == ActiveMQServer.SERVER_STATE.STOPPED || state == ActiveMQServer.SERVER_STATE.STOPPING) {
                return;
            }

            // We need to call this here, this gives any dependent server a chance to deploy its own addresses
            // this needs to be done before clustering is fully activated
            nodeManagerActivateCallback.activated();
        }

        public void completeActivation() throws Exception {
            state = ActiveMQServerImpl.SERVER_STATE.STARTED;
            activationLatch.countDown();
            nodeManagerActivateCallback.activationComplete();
        }
    }
}