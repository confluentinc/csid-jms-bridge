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

public class KNodeManager extends NodeManager {

    private static final StructuredLogger SLOG = StructuredLogger.with(b -> b
            .loggerClass(KNodeManager.class));

    private final JmsBridgeConfiguration config;
    private final KafkaIntegration kafkaIntegration;

    private LeaderAssignmentManager assignmentManager;
    private KafkaLeaderElector<LeaderAssignment, HostMemberIdentity> leaderElector;

    private boolean isPreferredLeader = false;
    public KNodeManager(
            JmsBridgeConfiguration config,
            KafkaIntegration kafkaIntegration,
            boolean replicatedBackup) {

        super(replicatedBackup);
        setUUID(kafkaIntegration.getNodeUuid());
        this.kafkaIntegration = kafkaIntegration;
        this.config = config;

        this.assignmentManager = new LeaderAssignmentManager();
        this.leaderElector = createLeaderElector();

        switch(config.getHAPolicyConfiguration().getType()) {
            case SHARED_STORE_MASTER: case LIVE_ONLY: case PRIMARY:
                isPreferredLeader = true;
                break;
            default:
                isPreferredLeader = false;
                break;
        }
    }

    private KafkaLeaderElector<LeaderAssignment, HostMemberIdentity> createLeaderElector() {
        HaConfig haConfig = config.getBridgeConfig().haConfig();
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(haConfig.groupId())
                .initTimeout(Duration.ofMillis(haConfig.initTimeoutMs()))
                .consumerConfigs(haConfig.consumerConfig())
                .build();


        JsonLeaderProtocol<LeaderAssignment, HostMemberIdentity> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), HostMemberIdentity.class, LeaderAssignment.class);

        HostMemberIdentity identity = HostMemberIdentity.builder()
                .host(getNodeId().toString())
                .build();

        KafkaLeaderElector<LeaderAssignment, HostMemberIdentity> elector =
                new KafkaLeaderElector<>(this.assignmentManager, protocol, props, identity);

        return elector;
    }

    @Override
    public synchronized void start() throws Exception {
        super.start();
        leaderElector.init();
        leaderElector.await();
        SLOG.info(
                b -> b.event("StartedNodeManager").markSuccess());
    }

    @Override
    public synchronized void stop() throws Exception {
        super.stop();
        leaderElector.close();
        SLOG.info(b -> b.event("StoppedNodeManager"));
    }

    @Override
    public void awaitLiveNode() {
        try {
            assignmentManager.awaitLeadership();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void awaitLiveStatus() {
        try {
            assignmentManager.awaitLeadership();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startBackup() {
        SLOG.info(b -> b.event("StartBackup"));
    }

  @Override
  public ActivateCallback startLiveNode() {
    SLOG.info(
        b -> b.event("StartLiveNode"));
    return new CleaningActivateCallback() {
      @Override
      public void activationComplete() {
          SLOG.warn(b -> b.event("StartLiveNode").markSuccess());
      }
    };
  }

    @Override
    public void pauseLiveServer() {
        SLOG.info(b -> b.event("PauseLiveServer"));
        try {
            kafkaIntegration.stopProcessor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void crashLiveServer() {
        SLOG.info(b -> b.event("CrashLiveServer"));
        try {
            kafkaIntegration.stopProcessor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isAwaitingFailback() {
        SLOG.info(
                b -> b.event("IsAwaitingFallback"));
        return isPreferredLeader && !assignmentManager.isLeader();
    }

    @Override
    public boolean isBackupLive() {
        return false;
    }

    public void interrupt() {
        //
    }

    @Override
    public void releaseBackup() {
        //
    }

    public void reset() throws Exception {
        //
    }

    @Override
    public SimpleString readNodeId() {
        return getNodeId();
    }

}

