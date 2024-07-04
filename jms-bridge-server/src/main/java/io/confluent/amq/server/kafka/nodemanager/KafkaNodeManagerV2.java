package io.confluent.amq.server.kafka.nodemanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.*;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * This supports the live/backup HA model. It's designed to work with 1 live and any number of backups.
 * HA is done by having a distributed lock that is used to determine which node is live and which node is the
 * designated backup. The mechanism used to implement the distributed lock is using Kafka Leader election protocol - as
 * used by consumer groups. The designated leader (assignment manager) is then handling lock assignments and cluster state
 * changes.
 * <p>
 * Any changes to cluster state, lock requests and releases go through rejoin group cycle to communicate with the designated
 * leader so that all the updates are done in a consistent manner and there are no races or concurrent competing updates.
 */
@Slf4j
public class KafkaNodeManagerV2 extends NodeManager implements AssignmentManager<LockAssignmentV2, LockMemberIdentityV2> {

    private final UUID nodeId;
    private final HaConfig haConfig;
    private Boolean isMasterPolicyNode;
    @Getter
    private volatile NodeLocks currentLock = NodeLocks.NONE;

    @Getter
    private volatile NodeLocks requestedLock = NodeLocks.NONE;

    @Getter
    private volatile ClusterStates currentState = ClusterStates.NOT_STARTED;
    private volatile boolean interrupt = false;
    @Getter
    private volatile boolean isAlive = false;

    private volatile ClusterState lastSeenClusterState = null;

    private final AtomicBoolean initialization = new AtomicBoolean(true);
    private CountDownLatch joinedLatch;
    private KafkaLeaderElector<LockAssignmentV2, LockMemberIdentityV2> leaderElector;

    /**
     * Unique identifier for the group member in the cluster - for leadership elector / lock assignment resolution
     */
    @Getter
    private String memberId = java.util.UUID.randomUUID().toString();

    private boolean stateUpdateRequested = false;

    ////////////////////////////////////////////
    // Node Manager implementation Below
    ///////////////////////////////////////////

    public KafkaNodeManagerV2(
            HaConfig haConfig,
            UUID nodeUuid,
            boolean replicatedBackup,
            boolean isMasterPolicyNode) {

        super(replicatedBackup);
        this.haConfig = haConfig;
        this.nodeId = nodeUuid;
        this.setNodeID(nodeUuid.toString());
        this.isMasterPolicyNode = isMasterPolicyNode;
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
        log.info("StartLiveNode >>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        this.isMasterPolicyNode = true;
        //Set shared cluster status to failingback to indicate any active backup (backup that was previously failed-over to)
        //that master/live node is ready for failback. Actual failback will be triggered depending on backup node configuration.
        //no further activation will happen until live lock is acquired (either because its not held by anyone or when backup releases it on fail-back or shutdown/termination)

        //Blocking until live lock is acquired and requested cluster state is set - can be interrupted by interrupt method.
        //Cluster state update is done exclusively by master node - so it will be performed immediately,
        // but the Live lock will be requested and only acquired once released by any other node (backup potentially)).
        performClusterStateChange(NodeLocks.LIVE, null, ClusterStates.FAILINGBACK);


        //Callback executed after the live lock is acquired and other activation steps complete
        //sets shared status to LIVE.
        //This call indicates that this is the live node
        return new CleaningActivateCallback() {
            @Override
            @SneakyThrows
            public void activationComplete() {
                log.debug("Activation complete callback. nodeId {}, memberId {}", getNodeId(), getMemberId());
                //update shared cluster state to Live - communicates to Backup that it can proceed with restart into backup mode.
                performClusterStateChange(null, null, ClusterStates.LIVE);
                log.debug("Activation as Live complete. nodeId {}, memberId {}", getNodeId(), getMemberId());
            }
        };
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
        log.info("startBackup >>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        this.isMasterPolicyNode = false;

        if (currentLock != NodeLocks.NONE) {
            log.warn("Node already has a lock, lock={}, nodeId {}, memberId {}", currentLock, getNodeId(), getMemberId());
        }

        //Acquire Backup lock - there can be multiple backup (slave policy) nodes - but only one should be active backup
        //guarded by the Backup lock.

        //Blocking until backup lock is acquired - can be interrupted by interrupt method.

        performClusterStateChange(NodeLocks.BACKUP, null, null);

        log.info("startBackup <<<<<<<<<<< acquired Backup lock, nodeId {}, memberId {}", getNodeId(), getMemberId());
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
        log.debug("awaitLiveNode >>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        do {
            while (currentState == ClusterStates.NOT_STARTED && isAlive) {
                log.info("Waiting for live node to initiate cluster state. nodeId {}, memberId {}", getNodeId(), getMemberId());
                Thread.sleep(10);
                checkInterrupt();
            }

            checkInterrupt();
            performClusterStateChange(NodeLocks.LIVE, null, null);

            if (!isAlive) {
                return;
            }

            log.debug("awaitLiveNode >>>>>>> currentLock: {} nodeId {}, memberId {}", currentLock, getNodeId(), getMemberId());
            if (currentState == ClusterStates.PAUSED) {
                log.debug("awaitLiveNode.PAUSED >>>>>>> currentLock: {} nodeId {}, memberId {}", currentLock, getNodeId(), getMemberId());
                performClusterStateChange(null, NodeLocks.LIVE, null);
                //release live lock - live node is paused / restarting - should not take over Live role.
            } else if (currentState == ClusterStates.FAILINGBACK) {
                log.debug("awaitLiveNode.FAILING_BACK >>>>>>> currentLock: {} nodeId {}, memberId {}", currentLock, getNodeId(), getMemberId());
                performClusterStateChange(null, NodeLocks.LIVE, null);
                //release live lock as racing with Live node activation / live node requested failback
            } else if (currentState == ClusterStates.LIVE) {
                break;
            }
        }
        while (true);

        log.debug("awaitLiveNode I'M ALIVVVVVEEE! <<<<<<<<< nodeId {}, memberId {}", getNodeId(), getMemberId());
    }

    /**
     * This method is used by the backup node to know when the live server finished activation on failback and it's ok to restart Backup node to go back into backup activation / initialization
     */
    @Override
    public void awaitLiveStatus() throws NodeManagerException, InterruptedException {
        //This is used during failback, it blocks until the live server updated Cluster State to LIVE.
        log.debug("AwaitLiveStatus >>>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        while (currentState != ClusterStates.LIVE && isAlive) {
            Thread.sleep(10);
        }
        log.debug("AwaitLiveStatus <<<<<<<<<< nodeId {}, memberId {}", getNodeId(), getMemberId());
    }

    /**
     * This method is used to gracefully bounce the live node. It informs backups that it will be back shortly and
     * they shouldn't acquire the live lock.
     */
    @Override
    @SneakyThrows
    public void pauseLiveServer() throws NodeManagerException {
        log.debug("PauseLiveServer >>>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        //Set cluster state to PAUSED and release Live lock if held
        performClusterStateChange(null, NodeLocks.LIVE == currentLock ? NodeLocks.LIVE : null, ClusterStates.PAUSED);
    }

    /**
     * Shutsdown the live server without intention of bringing it back up immediately.
     */
    @Override
    public void crashLiveServer() throws NodeManagerException {
        log.debug("CrashLiveServer >>>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        if (currentLock == NodeLocks.LIVE) {
            try {
                performClusterStateChange(null, NodeLocks.LIVE, null);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Releases the backup lock.
     */
    @Override
    public void releaseBackup() throws NodeManagerException {
        //release backup lock if held
        if (currentLock == NodeLocks.BACKUP || (lastSeenClusterState != null && lastSeenClusterState.getBackupLock() != null && lastSeenClusterState.getBackupLock().getLockHolderId().equals(getMemberId().toString()))) {
            try {
                performClusterStateChange(null, NodeLocks.BACKUP, null);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SimpleString readNodeId() throws NodeManagerException {
        return SimpleString.toSimpleString(this.nodeId.toString());
    }

    /**
     * True if cluster status is FAILBACK, indicates Live(Master) node is ready for failback and is communicating to
     * Backup (Slave) node to start the failback process if enabled. The failback will only happen if backup node has
     * it enabled in its configuration.
     */
    @Override
    public boolean isAwaitingFailback() throws NodeManagerException {
        log.debug("isAwaitingFailback >>>>>>>> nodeId {}, memberId {}, is {}", getNodeId(), getMemberId(), currentState == ClusterStates.FAILINGBACK);
        return currentState == ClusterStates.FAILINGBACK;
    }

    /**
     * Live node if peeking at cluster state to see if any node (other than this) is holding the live lock already.
     * For example Backup that assumed live role due to previous failover
     *
     * @return
     * @throws NodeManagerException
     */
    @Override
    @SneakyThrows
    public boolean isBackupLive() throws NodeManagerException {
        log.debug("isBackupLive >>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        this.isMasterPolicyNode = true;

        //Not connected yet / haven't got an assignment yet - do a dummy cluster state change request to get current state
        while (leaderElector == null || lastSeenClusterState == null) {
            performClusterStateChange(null, null, null);
        }

        //is live lock held by any node in the cluster (excluding this node)
        boolean result = lastSeenClusterState.getLiveLock() != null && !lastSeenClusterState.getLiveLock().getLockHolderId().equals(getMemberId());
        //not aquiring live lock - so shouldnt need to release - just peeking at cluster state...

        log.debug("isBackupLive <<<<<<<<< nodeId {}, memberId {}", getNodeId(), getMemberId());
        return result;
    }

    @Override
    public synchronized void start() throws Exception {
        log.debug("start >>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        if (leaderElector != null) {
            log.warn("NodeManager start is called on already active NodeManager, nodeId {}, memberId {}", getNodeId(), getMemberId());
        } else {
            //Join the cluster, but not request anything yet.
            performClusterStateChange(null, null, null);
        }
        super.start();
        isAlive = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        log.debug("stop >>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        super.stop();
        isAlive = false;
        if (this.joinedLatch != null) {
            this.joinedLatch.countDown();
            this.joinedLatch = null;
        }
        if (this.leaderElector != null) {
            this.leaderElector.close();
            this.leaderElector = null;

        }
    }


    @Override
    public void interrupt() {
        log.debug("Interrupt >>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        //unlock latch
        if (joinedLatch != null) {
            joinedLatch.countDown();
        }
        //need to be volatile: must be called concurrently to work as expected
        interrupt = true;
    }

    private void requestClusterStateChange(NodeLocks requestedLock, NodeLocks releasedLock, ClusterStates requestedClusterState) throws LeaderException, InterruptedException {
        requestClusterStateChange(requestedLock, releasedLock, requestedClusterState, false);
    }

    private void requestClusterStateChange(NodeLocks requestedLock, NodeLocks releasedLock, ClusterStates requestedClusterState, boolean stateUpdate) throws LeaderException, InterruptedException {
        if (joinedLatch != null && !stateUpdate) {
            joinedLatch.await();
        }
        LockMemberIdentityV2.LockMemberIdentityV2Builder identityV2Builder = LockMemberIdentityV2.builder()
                .id(getMemberId())
                .masterPolicy(isMasterPolicyNode);
        if (requestedLock != null) {
            identityV2Builder.requestLock(requestedLock);
        }
        if (releasedLock != null) {
            identityV2Builder.releaseLock(releasedLock);
        }
        if (lastSeenClusterState != null) {
            identityV2Builder.clusterState(lastSeenClusterState);
        }
        if (requestedClusterState != null) {
            identityV2Builder.requestClusterState(requestedClusterState.name());
        }
        LockMemberIdentityV2 identityV2 = identityV2Builder.build();

        if (!stateUpdate) {
            joinedLatch = new CountDownLatch(1);
            log.trace("Join latch set. nodeId {}, memberId {}", getNodeId(), getMemberId());
        }
        if (this.leaderElector == null) {
            this.leaderElector = createLeaderElector(identityV2);
            this.leaderElector.init();
        } else {
            leaderElector.forceRejoin(identityV2);
        }
        try {
            leaderElector.await();
        } catch (LeaderTimeoutException e) {
            throw new NodeManagerException(e);
        }
    }

    private void performClusterStateChange(NodeLocks requestedLock, NodeLocks releasedLock, ClusterStates requestedClusterState) throws LeaderException, InterruptedException {
        requestClusterStateChange(requestedLock, releasedLock, requestedClusterState);
        //Only wail and clear requests if there was anything requested in the first place, otherwise it is just an initial join / connect to listen to existing states
        if (requestedLock != null || releasedLock != null || requestedClusterState != null) {
            if (releasedLock != null) {
                spinUntilReleasedOrInterrupted(releasedLock, requestedClusterState);
            } else {
                spinUntilAcquiredOrInterrupted(requestedLock, requestedClusterState);
            }
            //cluster state change performed - clear the requests by rejoining again
            requestClusterStateChange(null, null, null);
        }
    }

    private KafkaLeaderElector<LockAssignmentV2, LockMemberIdentityV2> createLeaderElector(
            LockMemberIdentityV2 identityV2) {
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(haConfig.groupId())
                .initTimeout(Duration.ofMillis(haConfig.initTimeoutMs()))
                .consumerConfigs(haConfig.consumerConfig())
                .build();

        JsonLeaderProtocol<LockAssignmentV2, LockMemberIdentityV2> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), LockMemberIdentityV2.class, LockAssignmentV2.class);

        return new KafkaLeaderElector<>(this, protocol, props, identityV2);

    }

    private void spinUntilAcquiredOrInterrupted(NodeLocks nodeLock, ClusterStates clusterState) throws InterruptedException {
        while (((nodeLock != null && currentLock != nodeLock) || (clusterState != null && this.currentState != clusterState)) && isAlive) {
            checkInterrupt();
            Thread.sleep(10);
        }
    }


    private void spinUntilReleasedOrInterrupted(NodeLocks nodeLock, ClusterStates clusterState) throws InterruptedException {
        while ((currentLock == nodeLock || (clusterState != null && this.currentState != clusterState)) && isAlive) {
            checkInterrupt();
            Thread.sleep(10);
        }
    }

    private void checkInterrupt() throws InterruptedException {
        if (interrupt) {
            interrupt = false;
            throw new InterruptedException("Awaiting Lock was interrupted");
        }
    }

    ////////////////////////////////////////////
    // Assignment Manager implementation Below
    ///////////////////////////////////////////
    @Override
    public void onAssigned(LockAssignmentV2 assignment, int generation) {
        log.debug("onAssigned >>>>>>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        log.trace("onAssigned >>>>>>>>>>>>> nodeId {}, memberId {}, assignment {}, generation {}", getNodeId(), getMemberId(), assignment, generation);

        if (assignment == null) {
            log.debug("onAssigned: assignment is null, nodeId {}, memberId {}", getNodeId(), getMemberId());
            joinedLatch.countDown();
            initialization.set(false);
            return;
        }
        if (assignment.isUpdateRequested()) {
            //assignment manager migrated and has stale state - update state through sending cached cluster state by doing rejoin
            log.debug("onAssigned: Update requested, rejoining group, nodeId {}, memberId {}", getNodeId(), getMemberId());
            try {
                requestClusterStateChange(null, null, null, true);
            } catch (LeaderException | InterruptedException e) {
                log.error("onAssigned: Error rejoining group, nodeId {}, memberId {}, error: ", getNodeId(), getMemberId(), e);
            }
        }
        this.lastSeenClusterState = assignment.getClusterState();
        this.currentLock = assignment.holdsLiveLock ? NodeLocks.LIVE : (assignment.holdsBackupLock ? NodeLocks.BACKUP : NodeLocks.NONE);
        this.currentState = assignment.getClusterState().getClusterState();
        log.trace("Join latch countdown, nodeId {}, memberId {}", getNodeId(), getMemberId());
        joinedLatch.countDown();
        initialization.set(false);
    }

    @Override
    public void onRevoked(Timer timer) {
        log.debug("onRevoked >>>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        initialization.set(true);
    }

    @Override
    public Map<LockMemberIdentityV2, LockAssignmentV2> assign(List<LockMemberIdentityV2> identities) {
        Map<LockMemberIdentityV2, LockAssignmentV2> results = new HashMap<>();
        log.debug("Assign >>>>>>>> nodeId {}, memberId {}", getNodeId(), getMemberId());
        log.trace("Assign: identities: {}", identities);

        //cannot have more than one master node
        long masterCount = identities.stream().filter(LockMemberIdentityV2::isMasterPolicy).count();
        if (masterCount > 1) {
            log.error("More than one master node found in the cluster. Master count: {}", masterCount);
            throw new IllegalStateException("More than one master node found in the cluster. Master count: " + masterCount);
        }

        // find highest last seen epoch in identities
        int maxEpoch = identities.stream().filter(id -> id.getClusterState() != null).map(id -> id.getClusterState().getEpoch()).max(Integer::compareTo).orElse(-1);

        int localStateEpoch = this.lastSeenClusterState == null ? -1 : this.lastSeenClusterState.getEpoch();
        if (localStateEpoch < maxEpoch) {
            if (stateUpdateRequested) {
                log.trace("Assign: local state stale - request state update already in progress: nodeId {}, memberId {}, stateEpoch: {}", getNodeId(), getMemberId(), localStateEpoch);
                identities.forEach(id -> {
                    results.put(id, null);
                });
            } else {
                log.trace("Assign: local state stale - request state update: nodeId {}, memberId {}, stateEpoch: {}", getNodeId(), getMemberId(), localStateEpoch);
                identities.forEach(id -> {
                    results.put(id, LockAssignmentV2.builder()
                            .version(1)
                            .error((short) 0)
                            .updateRequested(true)
                            .build());
                });
                log.trace("Assign: nodeId {}, memberId {}, results: {}", getNodeId(), getMemberId(), results);
                stateUpdateRequested = true;
                return results;
            }
        }

        final ClusterState lastState = this.lastSeenClusterState != null ? this.lastSeenClusterState : ClusterState.builder().epoch(0).clusterState(ClusterStates.NOT_STARTED).build();
        log.trace("Assign: nodeId {}, memberId {}, lastState: {}", getNodeId(), getMemberId(), lastState);

        lastState.setEpoch(lastState.getEpoch() + 1);
        //Clear out identities from lastState that are not present in membership assignment now.
        if (lastState.getLiveLock() != null && identities.stream().noneMatch(id -> id.getId().equals(lastState.getLiveLock().getLockHolderId()))) {
            log.debug("Live lock released as holder not present in group assignment, nodeId {}, memberId {}, was held by id: {}", getNodeId(), getMemberId(), lastState.getLiveLock().getLockHolderId());
            lastState.setLiveLock(null);
        }
        if (lastState.getBackupLock() != null && identities.stream().noneMatch(id -> id.getId().equals(lastState.getBackupLock().getLockHolderId()))) {
            log.debug("Backup lock released as holder not present in group assignment nodeId {}, memberId {}, was held by id: {}", getNodeId(), getMemberId(), lastState.getBackupLock().getLockHolderId());
            lastState.setBackupLock(null);
        }

        List<String> liveLockRequesters = identities.stream().filter(id -> id.getRequestLock() == NodeLocks.LIVE).map(LockMemberIdentityV2::getId).collect(Collectors.toList());
        List<String> backupLockRequesters = identities.stream().filter(id -> id.getRequestLock() == NodeLocks.BACKUP).map(LockMemberIdentityV2::getId).collect(Collectors.toList());

        lastState.setLiveLockRequesters(liveLockRequesters);
        lastState.setBackupLockRequesters(backupLockRequesters);

        Optional<LockMemberIdentityV2> newClusterStateRequester = identities.stream().filter(id -> id.getRequestClusterState() != null && !lastState.getClusterState().name().equals(id.getRequestClusterState())).findFirst();
        newClusterStateRequester.ifPresent(clusterStateRequester -> {
            log.debug("Assign: nodeId {}, memberId {}. ClusterState change: {} -> {}", getNodeId(), getMemberId(), lastState.getClusterState(), ClusterStates.valueOf(clusterStateRequester.getRequestClusterState()));
            lastState.setClusterState(ClusterStates.valueOf(clusterStateRequester.getRequestClusterState()));
            //Reset cluster state change request as fulfilled
            clusterStateRequester.setRequestClusterState(null);
        });

        identities.stream().filter(id -> id.getReleaseLock() != null).forEach(id -> {
            if (id.getReleaseLock() == NodeLocks.LIVE) {
                if (lastState.getLiveLock() != null && lastState.getLiveLock().getLockHolderId().equals(id.getId())) {
                    log.debug("Releasing Live lock on request, nodeId {}, memberId {}, requester id: {}", getNodeId(), getMemberId(), id.getId());
                    lastState.setLiveLock(null);
                    //Reset release request
                    id.setReleaseLock(null);
                } else {
                    if (lastState.getLiveLock() == null) {
                        log.warn("Requested to release Live lock but it is not held by any node, nodeId {}, memberId {}, requester id: {}", getNodeId(), getMemberId(), id.getId());
                    } else {
                        log.warn("Requested to release Live lock but it is held by a different node, nodeId {}, memberId {}, requester id: {}, held by id: {}", getNodeId(), getMemberId(), id.getId(), lastState.getLiveLock().getLockHolderId());
                    }
                }
            } else if (id.getReleaseLock() == NodeLocks.BACKUP) {
                if (lastState.getBackupLock() != null && lastState.getBackupLock().getLockHolderId().equals(id.getId())) {
                    log.debug("Releasing Backup lock on request, nodeId {}, memberId {}, requester id: {}", getNodeId(), getMemberId(), id.getId());
                    lastState.setBackupLock(null);
                    //Reset release request
                    id.setReleaseLock(null);
                } else {
                    if (lastState.getBackupLock() == null) {
                        log.warn("Requested to release Backup lock but it is not held by any node, nodeId {}, memberId {}, requester id: {}", getNodeId(), getMemberId(), id.getId());
                    } else {
                        log.warn("Requested to release Backup lock but it is held by a different node, nodeId {}, memberId {}, requester id: {}, held by id: {}", getNodeId(), getMemberId(), id.getId(), lastState.getBackupLock().getLockHolderId());
                    }
                }
            }
        });
        //If Live lock available - check if any node wants it
        if (lastState.getLiveLock() == null) {
            if (!liveLockRequesters.isEmpty()) {
                log.debug("Assigned Live lock, nodeId {}, memberId {}, assigned to id: {}", getNodeId(), getMemberId(), liveLockRequesters.get(0));
                identities.stream().filter(id -> id.getId().equals(liveLockRequesters.get(0))).findFirst().ifPresent(id -> id.setRequestLock(null));
                lastState.setLiveLock(AcquiredLock.builder().lockHolderId(liveLockRequesters.get(0)).acquisitionEpoch(lastState.getEpoch()).build());
            }
        }
        //If Backup lock available - check if any node wants it
        if (lastState.getBackupLock() == null) {
            if (!backupLockRequesters.isEmpty()) {
                log.debug("Assigned Backup lock, nodeId {}, memberId {}, assigned to id: {}", getNodeId(), getMemberId(), backupLockRequesters.get(0));
                identities.stream().filter(id -> id.getId().equals(backupLockRequesters.get(0))).findFirst().ifPresent(id -> id.setRequestLock(null));
                lastState.setBackupLock(AcquiredLock.builder().lockHolderId(backupLockRequesters.get(0)).acquisitionEpoch(lastState.getEpoch()).build());
            }
        }

        identities.forEach(id -> {
            results.put(id, LockAssignmentV2.builder()
                    .version(1)
                    .error((short) 0)
                    .clusterState(lastState)
                    .holdsLiveLock(lastState.getLiveLock() != null && lastState.getLiveLock().getLockHolderId().equals(id.getId()))
                    .holdsBackupLock(lastState.getBackupLock() != null && lastState.getBackupLock().getLockHolderId().equals(id.getId()))
                    .build());
        });
        log.trace("Assign: nodeId {}, memberId {}, results: {}", getNodeId(), getMemberId(), results);
        log.debug("Assign <<<<<<<<<< nodeId {}, memberId {}", getNodeId(), getMemberId());
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

    public void reset() {
        //noop
    }
}
