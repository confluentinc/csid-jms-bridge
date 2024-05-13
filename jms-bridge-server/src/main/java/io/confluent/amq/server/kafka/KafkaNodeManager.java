package io.confluent.amq.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.germanosin.kafka.leader.*;
import com.github.germanosin.kafka.leader.tasks.LockAssignment;
import io.confluent.amq.config.HaConfig;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.activemq.artemis.core.server.impl.InVMNodeManager.State.LIVE;


@Slf4j
public class KafkaNodeManager extends NodeManager implements AssignmentManager<LockAssignment, LockMemberIdentity> {
    enum NodeLocks {
        LIVE, BACKUP, NONE
    }

    private final UUID nodeId;
    private final HaConfig haConfig;
    private Boolean isPreferredLeader;
    private final AtomicBoolean isBackupLive = new AtomicBoolean(false);
    private volatile NodeLocks currentLock = NodeLocks.NONE;
    private volatile boolean interrupt = false;
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

    private KafkaLeaderElector<LockAssignment, LockMemberIdentity> createLeaderElector(boolean isPreferredLiveNode) {
        KafkaLeaderProperties props = KafkaLeaderProperties.builder()
                .groupId(haConfig.groupId())
                .initTimeout(Duration.ofMillis(haConfig.initTimeoutMs()))
                .consumerConfigs(haConfig.consumerConfig())
                .build();

        JsonLeaderProtocol<LockAssignment, LockMemberIdentity> protocol =
                new JsonLeaderProtocol<>(new ObjectMapper(), LockMemberIdentity.class, LockAssignment.class);

        LockMemberIdentity identity = LockMemberIdentity.builder()
                .id(getNodeId().toString())
                .preferredLock(isPreferredLiveNode ? NodeLocks.LIVE.name() : NodeLocks.BACKUP.name())
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
                currentLock = NodeLocks.NONE;
            }
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
    }

    @Override
    public void startBackup() throws NodeManagerException, InterruptedException {
        //this call indicates this is the backup
        if (isPreferredLeader == null) {
            isPreferredLeader = false;
        }

        log.info("startBackup >>>>>>>>> {}", getNodeId());
        synchronized (this) {
            if (this.leaderElector == null) {
                this.leaderElector = createLeaderElector(false);
            }
        }

        this.leaderElector.init();
        try {
            this.leaderElector.await();
        } catch (LeaderTimeoutException e) {
            throw new NodeManagerException(e + " NodeId: " + getNodeId());
        }

        while (currentLock != NodeLocks.BACKUP) {
            Thread.sleep(100);
            if (interrupt) {
                interrupt = false;
                throw new InterruptedException("Awaiting Live Lock was interrupted");
            }
        }
    }

    @Override
    public ActivateCallback startLiveNode() throws NodeManagerException, InterruptedException {
        log.info("StartLiveNode >>>>>>>> {}", getNodeId());
        if (isPreferredLeader == null) {
            isPreferredLeader = true;
        }
        synchronized (this) {
            if (this.leaderElector == null) {
                this.leaderElector = createLeaderElector(true);
            }
        }

        this.leaderElector.init();
        try {
            this.leaderElector.await();
        } catch (LeaderTimeoutException e) {
            throw new NodeManagerException(e);
        }

        //This call indicates that this is the live node
        return new CleaningActivateCallback() {
            @Override
            public void activationComplete() {
//                state = State.LIVE;
                log.debug("Activation as Live complete. NodeId: {}", getNodeId());
            }
        };
    }

    /**
     * This method is used by backup servers to wait for the moment they acquire the LIVE lock.
     */
    @Override
    public void awaitLiveNode() throws NodeManagerException, InterruptedException {
        log.debug("awaitLiveNode >>>>>>> {}", getNodeId());
        do {
            if (interrupt) {
                interrupt = false;
                throw new InterruptedException("Awaiting Live Lock was interrupted");
            }

            if (currentLock == NodeLocks.LIVE) {
                break;
            }


            if (!isAlive) {
                log.debug("Shutdown has been encountered, exiting awaitLiveNode, nodeId: {}", getNodeId());
                break;
            }
            log.debug("awaitLiveNode, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
            Thread.sleep(1000);
//            if (state == State.PAUSED) {
//                log.debug("awaitLiveNode.PAUSED, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
//                releaseLeaderElector();
//                Thread.sleep(10);
//            } else if (state == State.FAILING_BACK) {
//                log.debug("awaitLiveNode.FAILING_BACK, currentLock: {} >>>>>>> {}", currentLock, getNodeId());
//                releaseLeaderElector();
//                Thread.sleep(10);
//            } else if (state == State.LIVE) {
//                break;
//            }
        }
        while (true);


        log.debug("awaitLiveNode I'M ALIVVVVVEEE! <<<<<<<<< {}", getNodeId());
    }

    @Override
    public void awaitLiveStatus() throws NodeManagerException, InterruptedException {
        log.debug("AwaitLiveStatus >>>>>>>>>>");
        while (currentLock != NodeLocks.LIVE) {
            Thread.sleep(1000);
        }
        log.debug("AwaitLiveStatus <<<<<<<<<<");
    }

    @Override
    public void pauseLiveServer() throws NodeManagerException {
        log.debug("PauseLiveServer >>>>>>>>>>");
        releaseLeaderElector();
    }

    @Override
    public void crashLiveServer() throws NodeManagerException {
        log.debug("CrashLiveServer >>>>>>>>>> {}", getNodeId());
        releaseLeaderElector();
    }

    @Override
    public void releaseBackup() throws NodeManagerException {
//        log.debug("releaseBackup >>>>>>>>>> {}", getNodeId());
//        releaseLeaderElector();
//        log.debug("releaseBackup <<<<<<<<< {}", getNodeId());
    }

    @Override
    public SimpleString readNodeId() throws NodeManagerException {
        return SimpleString.toSimpleString(this.nodeId.toString());
    }

    @Override
    public boolean isAwaitingFailback() throws NodeManagerException {
        log.debug("isAwaitingFailback >>>>>>>> {}", getNodeId());
        return (isPreferredLeader && currentLock != NodeLocks.LIVE)
                || (!isPreferredLeader && currentLock == NodeLocks.LIVE);
    }

    @Override
    public boolean isBackupLive() throws NodeManagerException {
        log.debug("isBackupLive >>>>>>>>");
        return isBackupLive.get();
    }

    @Override
    public void interrupt() {
        log.debug("Interrupt >>>>>>>>");
        //need to be volatile: must be called concurrently to work as expected
        interrupt = true;
    }

    ////////////////////////////////////////////
    // Assignment Manager implementation Below
    ///////////////////////////////////////////
    @Override
    public void onAssigned(LockAssignment assignment, int generation) {
        log.debug("onAssigned >>>>>>>>>>>>> {}", getNodeId());
        if (assignment == null) {
            log.debug("onAssigned, null Assignment >>>>>>>>>>>>> {}", getNodeId());
            return;
        }

        if (assignment.getAcquiredLocks().contains(NodeLocks.BACKUP.name())) {
            currentLock = NodeLocks.BACKUP;
            log.debug("Obtained backup lock. NodeId: {}", getNodeId());
        }

        if (assignment.getAcquiredLocks().contains(NodeLocks.LIVE.name())) {
            log.debug("Obtained live lock. NodeId: {}", getNodeId());
            currentLock = NodeLocks.LIVE;
        }

        isBackupLive.set(assignment.isBackupLive());
        joinedLatch.countDown();
        initialization.set(false);
    }

    @Override
    public void onRevoked(Timer timer) {
        log.debug("onRevoked >>>>>>>>> {}", getNodeId());
        currentLock = NodeLocks.NONE;
        initialization.set(true);
    }

    @Override
    public Map<LockMemberIdentity, LockAssignment> assign(List<LockMemberIdentity> identities) {
        Map<LockMemberIdentity, LockAssignment> results = new HashMap<>();
        Map<LockMemberIdentity, Set<String>> memberLocks = new HashMap<>();

        //Find leader
        LockMemberIdentity leader = identities
                .stream()
                .filter(i -> NodeLocks.LIVE.name().equals(i.getPreferredLock()))
                .findFirst()
                .orElseGet(() -> {
                    log.debug("Backup is elected as leader. NodeId: {}", getNodeId());
                    return identities.get(0);
                });
        memberLocks.put(leader, Set.of(NodeLocks.LIVE.name()));

        //find backups
        List<LockMemberIdentity> backups = identities
                .stream()
                .filter(i -> !leader.getId().equals(i.getId()))
                .collect(Collectors.toList());
        backups.forEach(b -> memberLocks.put(b, Set.of(NodeLocks.BACKUP.name())));

        LockAssignment template = LockAssignment.builder()
                .version(1)
                .backupLive(LIVE.name().equals(leader.getPreferredLock()))
                .error((short) 0)
                .build();

        results.put(leader, template.toBuilder()
                .acquiredLocks(Set.of(NodeLocks.LIVE.name()))
                .build());

        backups.forEach(b -> results.put(b, template.toBuilder()
                .acquiredLocks(Set.of(NodeLocks.BACKUP.name()))
                .build()));

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
