package io.confluent.amq.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.*;
import com.github.germanosin.kafka.leader.amq.LockAssignment;
import io.confluent.amq.config.HaConfig;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * This supports the live/backup HA model. It's designed to work with 1 live and any number of backups.
 * HA is done by having a distributed lock that is used to determine which node is live and which node is the
 * designated backup. The mechanism used to implement the distributed lock is the same that is used for Kafka
 * consumer groups.
 * <p>
 * Besides the distributed locks there is also a cluster state that is passed along. Cluster state can only be set
 * by the designated live node and will be part of the member identity. In order to set the state the live node must
 * rejoin the group which entail first leaving it. For most purposes this is fine but for the Paused
 * state it requires the live node to leave and rejoin to set the state and then leave until it has finished rebooting.
 * This creates a race condition in which the backup acquires the live lock before the live node can inform it that
 * it is merely pausing.
 * <p>
 * To try to get around the race condition a delay on the backup side will be configured to allow the live node time to
 * update the state to paused.
 */
@Slf4j
public class KafkaNodeManager extends NodeManager implements AssignmentManager<LockAssignment, LockMemberIdentity> {
    enum NodeLocks {
        LIVE, BACKUP, NONE, NOT_ATTEMPTED
    }

    enum ClusterStates {
        LIVE, FAILINGBACK, PAUSED, NOT_STARTED
    }

    private final UUID nodeId;
    private final HaConfig haConfig;
    private Boolean isPreferredLeader;
    @Getter
    private volatile NodeLocks currentLock = NodeLocks.NOT_ATTEMPTED;
    @Getter
    private volatile ClusterStates currentState = ClusterStates.NOT_STARTED;
    private volatile boolean interrupt = false;
    @Getter
    private volatile boolean isAlive = false;

    private final AtomicBoolean initialization = new AtomicBoolean(true);
    private CountDownLatch joinedLatch;
    private KafkaLeaderElector<LockAssignment, LockMemberIdentity> leaderElector;

    ////////////////////////////////////////////
    // Node Manager implementation Below
    ///////////////////////////////////////////

    public KafkaNodeManager(
            HaConfig haConfig,
            UUID nodeUuid,
            boolean replicatedBackup) {

        super(replicatedBackup);
        this.haConfig = haConfig;
        this.nodeId = nodeUuid;
        this.setNodeID(nodeUuid.toString());
    }

    private KafkaLeaderElector<LockAssignment, LockMemberIdentity> createLeaderElector(
            boolean isPreferredLiveNode) {
        return createLeaderElector(isPreferredLiveNode, null);
    }

    private KafkaLeaderElector<LockAssignment, LockMemberIdentity> createLeaderElector(
            boolean isPreferredLiveNode, ClusterStates state) {
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(haConfig.groupId())
                .initTimeout(Duration.ofMillis(haConfig.initTimeoutMs()))
                .consumerConfigs(haConfig.consumerConfig())
                .build();

        JsonLeaderProtocol<LockAssignment, LockMemberIdentity> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), LockMemberIdentity.class, LockAssignment.class);

        LockMemberIdentity identity = LockMemberIdentity.builder()
                .id(getNodeId().toString())
                .preferredLive(isPreferredLiveNode)
                .clusterState(isPreferredLiveNode && state != null ? state.name() : null)
                .build();

        joinedLatch = new CountDownLatch(1);
        return new KafkaLeaderElector<>(this, protocol, props, identity);

    }

    private void releaseLeaderElector() {
        log.debug("releaseLeaderElector >>>>>>>>>");
        synchronized (this) {
            if (leaderElector != null) {
                leaderElector.close();
                leaderElector = null;
            }
        }
    }

    //Leave the group, rejoin with the new status and wait to obtain the current lock
    private void updateClusterStatus(ClusterStates newState) throws InterruptedException {
        //maintain current lock
        //need to drop out of the group
        //need to join the group with updated cluster status
        this.leaderElector.forceRejoin(LockMemberIdentity.builder()
                        .id(getNodeId().toString())
                        .preferredLive(this.isPreferredLeader)
                        .clusterState(newState.name())
                        .build());
    }

    private void spinUntilAcquiredOrInterrupted(NodeLocks nodeLock) throws InterruptedException {
        while (currentLock != nodeLock && isAlive) {
            checkInterrupt();
            Thread.sleep(1000);
        }
    }

    @Override
    public synchronized void start() throws Exception {
        log.debug("start >>>>>>>>> {}", getNodeId());
        super.start();
        isAlive = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        log.debug("stop >>>>>>>>> {}", getNodeId());
        super.stop();
        isAlive = false;
        if (this.leaderElector != null) {
            this.leaderElector.close();
            this.leaderElector = null;
        }
    }

    /**
     * This method is used to have the backup join the consumer group (in order to participate in lock assignments)
     * and to wait for the acquisition of the Backup lock. There can only be one designated backup but there may be
     * many nodes that could become a backup.
     * <p>
     * After this method returns the next method called for a backup node is {@link #awaitLiveNode()}.
     */
    @Override
    public void startBackup() throws NodeManagerException, InterruptedException {
        //this call indicates this is the backup
        isPreferredLeader = false;

        log.debug("startBackup >>>>>>>>> {}", getNodeId());
        synchronized (this) {
            if (this.leaderElector != null) {
                this.leaderElector.close();
            }
            this.leaderElector = createLeaderElector(false);
        }

        this.leaderElector.init();
        while ((currentLock != NodeLocks.BACKUP || currentState == ClusterStates.NOT_STARTED) && isAlive) {
            checkInterrupt();
            Thread.sleep(100);
        }
        log.info("startBackup <<<<<<<<<<< {}", getNodeId());
    }

    /**
     * This method is called by the designated live node to join the consumer group and acquire the live lock. When the
     * live node initially joins it sets the cluster state to FAILBACK, which lets backup nodes know that live is
     * alive again and wants to reclaim the live lock (this is only true if failback is set to true). Once joined
     * to the group with the updated status, live will then wait until it has acquired the live lock.
     * <p>
     * Once the live lock is acquired this method returns and the rest of the broker bootstraps. This includes
     * loading data from the state tables in Kafka.
     * <p>
     * A call back is returned that is used to hook a cluster status update upon the completion of the broker
     * bootstrap.
     */
    @Override
    public ActivateCallback startLiveNode() throws NodeManagerException, InterruptedException {
        log.info("StartLiveNode >>>>>>>> {}", getNodeId());
        this.isPreferredLeader = true;
        synchronized (this) {
            if (this.leaderElector != null) {
                this.leaderElector.close();
            }
            this.leaderElector = createLeaderElector(true, ClusterStates.FAILINGBACK);
        }
        this.leaderElector.init();

        spinUntilAcquiredOrInterrupted(NodeLocks.LIVE);


        //This call indicates that this is the live node
        return new CleaningActivateCallback() {
            @Override
            @SneakyThrows
            public void activationComplete() {
                while (currentState != ClusterStates.LIVE && isAlive) {
                    //this update can take multiple attempts for some reason.
                    updateClusterStatus(ClusterStates.LIVE);
                    Thread.sleep(1000);
                }
                log.debug("Activation as Live complete. NodeId: {}", getNodeId());
            }
        };
    }

    /**
     * This method is used by backup servers to wait for the moment they acquire the LIVE lock. Before initiating
     * bootstrap though the cluster status is checked, if it is in either PAUSED or FAILBACK then the live lock will
     * be released.
     * <p>
     * There is an added delay before a backup can go live, this allows the designated live server time to update the
     * cluster status.
     */
    @Override
    public void awaitLiveNode() throws NodeManagerException, InterruptedException {
        log.debug("awaitLiveNode >>>>>>> {}", getNodeId());
        do {
            while (currentState == ClusterStates.NOT_STARTED && isAlive) {
                log.info("Waiting for live node to initiate cluster state.");
                Thread.sleep(1000);
                checkInterrupt();
            }

            checkInterrupt();
            while (currentLock != NodeLocks.LIVE && isAlive) {
                checkInterrupt();
                Thread.sleep(1000);
            }

            if (!isAlive) {
                return;
            }

            //give live some time to update cluster status
            Thread.sleep(haConfig.initTimeoutMs());

            log.debug("awaitLiveNode, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
            if (currentState == ClusterStates.PAUSED) {
                log.debug("awaitLiveNode.PAUSED, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
            } else if (currentState == ClusterStates.FAILINGBACK) {
                log.debug("awaitLiveNode.FAILING_BACK, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
            } else if (currentState == ClusterStates.LIVE) {
                break;
            }
        }
        while (true);

        log.debug("awaitLiveNode I'M ALIVVVVVEEE! <<<<<<<<< {}", getNodeId());
    }

    /**
     * This method is used by the backup node to know when the live server comes back online and updates the cluster
     * status to FAILBACK.
     */
    @Override
    public void awaitLiveStatus() throws NodeManagerException, InterruptedException {
        //This is used during failback, it blocks until the live server is ready
        log.debug("AwaitLiveStatus >>>>>>>>>>");
        while (currentLock != NodeLocks.LIVE && isAlive) {
            Thread.sleep(1000);
        }
        log.debug("AwaitLiveStatus <<<<<<<<<<");
    }

    /**
     * This method is used to gracefully bounce the live node. It informs backups that it will be back shortly and
     * they shouldn't acquire the live lock.
     */
    @Override
    @SneakyThrows
    public void pauseLiveServer() throws NodeManagerException {
        log.debug("PauseLiveServer >>>>>>>>>>");
        while (currentState != ClusterStates.PAUSED && isAlive) {
            updateClusterStatus(ClusterStates.PAUSED);
            Thread.sleep(1000);
        }
        if (this.leaderElector != null) {
            this.leaderElector.close();
            this.leaderElector = null;
        }
    }

    /**
     * Shutsdown the live server without intention of bringing it back up immediately.
     */
    @Override
    public void crashLiveServer() throws NodeManagerException {
        log.debug("CrashLiveServer >>>>>>>>>> {}", getNodeId());
        releaseLeaderElector();
    }

    /**
     * Releases the backup lock.
     */
    @Override
    public void releaseBackup() throws NodeManagerException {
        if (currentLock == NodeLocks.BACKUP) {
            releaseLeaderElector();
        }
    }

    @Override
    public SimpleString readNodeId() throws NodeManagerException {
        return SimpleString.toSimpleString(this.nodeId.toString());
    }

    /**
     * True if cluster status is FAILBACK, indicates backup is waiting for live to become active.
     */
    @Override
    public boolean isAwaitingFailback() throws NodeManagerException {
        log.debug("isAwaitingFailback >>>>>>>> {}", getNodeId());
        return currentState == ClusterStates.FAILINGBACK;

    }

    /**
     * In this implementation this method forces the live node to join the group. It's illegal for the live node
     * to acquire the live lock while cluster state is LIVE. In that case the backup node is live.
     *
     * @return
     * @throws NodeManagerException
     */
    @Override
    @SneakyThrows
    public boolean isBackupLive() throws NodeManagerException {
        log.debug("isBackupLive >>>>>>>>");
        this.leaderElector = createLeaderElector(true);
        this.leaderElector.init();

        //this lock will update once we join the cluster
        while (currentLock == NodeLocks.NOT_ATTEMPTED && isAlive) {
            Thread.sleep(1000);
        }

        //if we aren't live then backup is
        return currentLock != NodeLocks.LIVE;
    }

    @Override
    public void interrupt() {
        log.debug("Interrupt >>>>>>>>");
        //need to be volatile: must be called concurrently to work as expected
        interrupt = true;
    }

    private void checkInterrupt() throws InterruptedException {
        if (interrupt) {
            interrupt = false;
            throw new InterruptedException("Awaiting Live Lock was interrupted");
        }
    }

    ////////////////////////////////////////////
    // Assignment Manager implementation Below
    ///////////////////////////////////////////
    @Override
    public void onAssigned(LockAssignment assignment, int generation) {
        log.debug("onAssigned >>>>>>>>>>>>> {}", getNodeId());
        if (assignment == null) {
            return;
        }

        if (assignment.getClusterState() != null) {
            currentState = ClusterStates.valueOf(assignment.getClusterState());
        }

        if (assignment.getAssignedLock() != null) {
            currentLock = NodeLocks.valueOf(assignment.getAssignedLock());
            //log.debug("Obtained {} lock. NodeId: {}", assignment.getAssignedLock(), getNodeId());
        }
        joinedLatch.countDown();
        initialization.set(false);
    }

    @Override
    public void onRevoked(Timer timer) {
        //log.debug("onRevoked >>>>>>>>> {}", getNodeId());
        initialization.set(true);
    }

    @Override
    public Map<LockMemberIdentity, LockAssignment> assign(List<LockMemberIdentity> identities) {
        //find a leader and promote it to live
        Map<LockMemberIdentity, LockAssignment> results = new HashMap<>();
        log.debug("Assign: ClusterState: {}", this.currentState);

        //Find leader
        Optional<LockMemberIdentity> preferredLiveOpt = identities
                .stream()
                .filter(LockMemberIdentity::isPreferredLive)
                .findFirst();

        //find backups
        List<LockMemberIdentity> backups = identities
                .stream()
                //filter out the leader
                .filter(i -> preferredLiveOpt.map(l -> !l.getId().equals(i.getId())).orElse(true))
                .collect(Collectors.toList());

        LockMemberIdentity leader = null;
        LockMemberIdentity backup = null;
        String clusterStatus = null;

        if (preferredLiveOpt.isEmpty()) {
            //here we promote a backup
            leader = backups.get(0);
            if (backups.size() > 1) {
                backup = backups.get(1);
            }
            clusterStatus = this.currentState != null ? this.currentState.name() : null;
        } else {
            leader = preferredLiveOpt.get();
            if (backups.size() > 0) {
                backup = backups.get(0);
            }
            clusterStatus = leader.getClusterState();
        }

        LockAssignment template = LockAssignment.builder()
                .version(1)
                .error((short) 0)
                .clusterState(clusterStatus)
                .build();

        for (LockMemberIdentity node: identities) {
            if (!node.getId().equals(leader.getId())
                    && backup != null
                    && !node.getId().equals(backup.getId())) {

                results.put(node, template.toBuilder()
                                .assignedLock(NodeLocks.NONE.name())
                        .build());
            }
        }

        if (leader != null) {
            results.put(leader, template.toBuilder()
                    .assignedLock(NodeLocks.LIVE.name())
                    .build());
        }

        if (backup != null) {
            results.put(backup, template.toBuilder()
                    .assignedLock(NodeLocks.BACKUP.name())
                    .build());
        }

        return results;
    }

    @Override
    public boolean isInitialisation() {
        return initialization.get();
    }

    @Override
    public boolean isAlive(Timer timer) {
        return isAlive;
    }

    @Override
    public boolean await(Duration timeout) throws LeaderTimeoutException, InterruptedException {
        return joinedLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        this.currentLock = NodeLocks.NONE;

    }
}
