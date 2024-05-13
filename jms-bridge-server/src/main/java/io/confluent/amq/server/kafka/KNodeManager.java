/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.*;
import com.github.germanosin.kafka.leader.tasks.DefaultLeaderTasksManager;
import com.github.germanosin.kafka.leader.tasks.PreferredLeaderTaskManager;
import com.github.germanosin.kafka.leader.tasks.Task;
import com.github.germanosin.kafka.leader.tasks.TaskAssignment;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.HaConfig;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;

import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.*;

public class KNodeManager extends NodeManager {
    public enum State {
        LIVE, PAUSED, FAILING_BACK, NOT_STARTED
    }

    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(KNodeManager.class));

    private final HaConfig config;

    private final boolean isPreferredLive;
    private volatile State state = State.NOT_STARTED;
    private volatile boolean isLeader = false;
    private volatile boolean isBackup = false;
    private KafkaLeaderElector<TaskAssignment, LockMemberIdentity> leaderElector;

    private Semaphore liveLock = new Semaphore(0);
    private Semaphore backupLock = new Semaphore(0);

    private boolean started = false;

    public long failoverPause = 0L;

    public KNodeManager(
            HaConfig haConfig,
            UUID nodeUuid,
            boolean replicatedBackup,
            boolean isPreferredLive) {

        super(replicatedBackup);
        setUUID(nodeUuid);
        this.config = haConfig;
//        this.leaderElector = createLeaderElector();
        this.isPreferredLive = isPreferredLive;

    }

    @Override
    protected synchronized void notifyLostLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void registerLockListener(LockListener lockListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void unregisterLockListener(LockListener lockListener) {
        throw new UnsupportedOperationException();
    }

    private KafkaLeaderElector<TaskAssignment, LockMemberIdentity> createLeaderElector(boolean isPreferredLeader) {
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(config.groupId())
                .initTimeout(Duration.ofMillis(config.initTimeoutMs()))
                .consumerConfigs(config.consumerConfig())
                .build();


        AssignmentManager<TaskAssignment, LockMemberIdentity> assignmentManager =
                new DefaultLeaderTasksManager<>(
                        Map.of("live", getLeaderTask()));

        JsonLeaderProtocol<TaskAssignment, LockMemberIdentity> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), LockMemberIdentity.class, TaskAssignment.class);

        LockMemberIdentity identity = LockMemberIdentity.builder()
                .id(getNodeId().toString())
                .build();

        return new KafkaLeaderElector<>(assignmentManager, protocol, props, identity);

    }

    @Override
    public synchronized void start() throws Exception {
        started = true;

        SLOG.info(
                b -> b.event("StartedNodeManager").markSuccess());
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void awaitLiveNode() throws InterruptedException {
        SLOG.logger().info(">>>>>>>>>>>> Await Live Node");
        do {
            while (state == State.NOT_STARTED) {
                Thread.sleep(10);
            }

            waitForLeadership();

            if (state == State.PAUSED) {
                SLOG.logger().info("Closing leaderElector, KNodeManager.awaitLiveNode1");
                leaderElector.close();
                Thread.sleep(10);
            } else if (state == State.FAILING_BACK) {
                SLOG.logger().info("Closing leaderElector, KNodeManager.awaitLiveNode2");
                leaderElector.close();
                Thread.sleep(10);
            } else if (state == State.LIVE) {
                break;
            }
        }
        while (true);
        if (failoverPause > 0L) {
            Thread.sleep(failoverPause);
        }
    }

    @Override
    public synchronized void stop() throws Exception {
        SLOG.logger().info(">>>>>>>>>>>> Stop");
        SLOG.logger().info("Closing leaderElector, KNodeManager.stop");
        leaderElector.close();
        SLOG.info(b -> b.event("StoppedNodeManager"));
    }

    @Override
    public void awaitLiveStatus() throws InterruptedException {
        SLOG.logger().info(">>>>>>>>>>>> Await Live Status");
        while (state != State.LIVE) {
            Thread.sleep(10);
        }
    }

    @Override
    public void startBackup() throws InterruptedException {
        SLOG.info(b -> b.event("StartBackup"));
        try {
            leaderElector.init();
            leaderElector.await();
        } catch (LeaderTimeoutException e) {
            throw new RuntimeException(e);
        }
        while (!isBackup) {
            Thread.sleep(10);
        }
    }

    @Override
    public ActivateCallback startLiveNode() throws InterruptedException {
        SLOG.logger().info(">>>>>>>>>>>> Start Live Node");
        state = State.FAILING_BACK;
        waitForLeadership();
        return new CleaningActivateCallback() {
            @Override
            public void activationComplete() {
                state = State.LIVE;
            }
        };

    }

    @Override
    public void crashLiveServer() {
        SLOG.logger().info("Closing leaderElector, KNodeManager.crashLiveServer");
        leaderElector.close();
    }

    @Override
    public boolean isAwaitingFailback() {
        SLOG.info(
                b -> b.event("IsAwaitingFallback"));
        return state == State.FAILING_BACK;
    }

    @Override
    public boolean isBackupLive() {
        return isBackup;
    }

    @Override
    public void pauseLiveServer() {
        state = State.PAUSED;
        SLOG.logger().info("Closing leaderElector, KNodeManager.pauseLiveServer");
        leaderElector.close();
    }
    public void interrupt() {
        //
    }

    @Override
    public void releaseBackup() {
        SLOG.logger().info(">>>>>>>>>>>> Release Backup");
        if (isBackup) {
            SLOG.logger().info("Closing leaderElector, KNodeManager.releaseBackup");
            leaderElector.close();
        }
    }

    public void reset() throws Exception {
        //
    }

    @Override
    public SimpleString readNodeId() {
        return getNodeId();
    }

    private void waitForLeadership() throws InterruptedException {
        try {
            leaderElector.init();
            leaderElector.await();
        } catch (LeaderTimeoutException e) {
            throw new RuntimeException(e);
        }

    }

    private Task getBackupTask() {
        return new Task() {
            CountDownLatch closeLatch;
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public boolean isStarted() {
                return isBackup;
            }

            @Override
            public void close() throws Exception {
                isBackup = false;
                if (closeLatch != null) {
                    closeLatch.countDown();
                }
            }

            @Override
            public void run() {

                SLOG.logger().info(">>>>>>>>>>>> Acquired Backup Task");
                isBackup = true;
                //clean up old flags
                isLeader = false;
                closeLatch = new CountDownLatch(1);
                try {
                    closeLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Task getLeaderTask() {
        return new Task() {
            CountDownLatch closeLatch;
            @Override
            public boolean isAlive() {
                return true;
            }

            @Override
            public boolean isStarted() {
                return isLeader;
            }

            @Override
            public void close() throws Exception {
                isLeader = false;
                if (closeLatch != null) {
                    closeLatch.countDown();
                }
            }

            @Override
            public void run() {

                SLOG.logger().info(">>>>>>>>>>>> Acquired Leader Task");
                isLeader = true;

                //clean up old flags
                isBackup = false;
                closeLatch = new CountDownLatch(1);
                try {
                    closeLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}

