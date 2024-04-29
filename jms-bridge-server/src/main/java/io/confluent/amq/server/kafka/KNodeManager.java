/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.*;
import com.github.germanosin.kafka.leader.tasks.DefaultLeaderTasksManager;
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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.*;

public class KNodeManager extends NodeManager {
    public enum State {
        LIVE, PAUSED, FAILING_BACK, NOT_STARTED
    }

    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(KNodeManager.class));

    private final JmsBridgeConfiguration config;
    private final KafkaIntegration kafkaIntegration;


    public volatile State state = State.NOT_STARTED;
    public volatile boolean isLeader = false;
    public volatile boolean isBackup = false;
    private KafkaLeaderElector<TaskAssignment, HostMemberIdentity> leaderElector;

    private boolean started = false;

    public long failoverPause = 0L;

    public KNodeManager(
            JmsBridgeConfiguration config,
            KafkaIntegration kafkaIntegration,
            boolean replicatedBackup) {

        super(replicatedBackup);
        setUUID(kafkaIntegration.getNodeUuid());
        this.kafkaIntegration = kafkaIntegration;
        this.config = config;
        this.leaderElector = createLeaderElector();

    }

    private KafkaLeaderElector<TaskAssignment, HostMemberIdentity> createLeaderElector() {
        HaConfig haConfig = config.getBridgeConfig().haConfig();
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(haConfig.groupId())
                .initTimeout(Duration.ofMillis(haConfig.initTimeoutMs()))
                .consumerConfigs(haConfig.consumerConfig())
                .build();


        AssignmentManager<TaskAssignment, HostMemberIdentity> assignmentManager =
                new DefaultLeaderTasksManager<>(Map.of("live", getLeaderTask()));

        JsonLeaderProtocol<TaskAssignment, HostMemberIdentity> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), HostMemberIdentity.class, TaskAssignment.class);

        HostMemberIdentity identity = HostMemberIdentity.builder()
                .host(getNodeId().toString())
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
        do {
            while (state == State.NOT_STARTED) {
                Thread.sleep(10);
            }

            waitForLeadership();

            if (state == State.PAUSED) {
                leaderElector.close();
                Thread.sleep(10);
            } else if (state == State.FAILING_BACK) {
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
        super.stop();
        leaderElector.close();
        SLOG.info(b -> b.event("StoppedNodeManager"));
    }

    @Override
    public void awaitLiveStatus() throws InterruptedException {
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
        leaderElector.close();
    }
    public void interrupt() {
        //
    }

    @Override
    public void releaseBackup() {
        if (isBackup) {
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
        while (!isLeader) {
            Thread.sleep(10);
        }
    }

    private Task getBackupTask() {
        return new Task() {
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
            }

            @Override
            public void run() {
                isBackup = true;
                //clean up old flags
                isLeader = false;
            }
        };
    }

    private Task getLeaderTask() {
        return new Task() {
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
            }

            @Override
            public void run() {
                isLeader = true;

                //clean up old flags
                isBackup = false;
            }
        };
    }
}

