package io.confluent.amq.server.kafka;

import io.confluent.amq.JmsBridgeConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.core.server.impl.CleaningActivateCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;
/**
 * Kafka implementation of {@link NodeManager}.
 */
public final class KafkaNodeManager extends NodeManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNodeManager.class);
    private static final long MAX_PAUSE_MILLIS = 2000L;

    private final Supplier<? extends SharedStateManager> sharedStateManagerFactory;
    private final Supplier<? extends ScheduledLeaseLock> scheduledLiveLockFactory;
    private final Supplier<? extends ScheduledLeaseLock> scheduledBackupLockFactory;
    private SharedStateManager sharedStateManager;
    private ScheduledLeaseLock scheduledLiveLock;
    private ScheduledLeaseLock scheduledBackupLock;

    private final long lockAcquisitionTimeoutMillis;
    private volatile boolean interrupted = false;
    private final LeaseLock.Pauser pauser;
    private final IOCriticalErrorListener ioCriticalErrorListener;

    public KafkaNodeManager(JmsBridgeConfiguration configuration,
                            ScheduledExecutorService scheduledPool,
                            ExecutorFactory executorFactory,
                            ActiveMQServerImpl.ShutdownOnCriticalErrorListener shutdownOnCriticalIO,
                            IOCriticalErrorListener ioCriticalErrorListener) {
        super();
        this.ioCriticalErrorListener = ioCriticalErrorListener;
        // ToDo validateTimeoutConfiguration(configuration);
        // Define how we will conect to Kafka

        // Create New Broker ID
        final String brokerId = java.util.UUID.randomUUID().toString();



    }

    // This method executes and implements the lock renewal methods implemented in the KafkaShared State Manager
    private KafkaNodeManager(Supplier<? extends SharedStateManager> sharedStateManagerFactory,
                             long lockRenewPeriodMillis,
                             long lockAcquisitionTimeoutMillis,
                             ScheduledExecutorService scheduledExecutorService,
                             ExecutorFactory executorFactory,
                             IOCriticalErrorListener ioCriticalErrorListener) {
        super(false, null);
        this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
        this.pauser = LeaseLock.Pauser.sleep(Math.min(lockRenewPeriodMillis, MAX_PAUSE_MILLIS), TimeUnit.MILLISECONDS);
        this.sharedStateManagerFactory = sharedStateManagerFactory;
        this.scheduledLiveLockFactory = () -> ScheduledLeaseLock.of(
                scheduledExecutorService,
                executorFactory != null ? executorFactory.getExecutor() : null,
                "live",
                this.sharedStateManager.liveLock(),
                lockRenewPeriodMillis,
                ioCriticalErrorListener);
        this.scheduledBackupLockFactory = () -> ScheduledLeaseLock.of(
                scheduledExecutorService,
                executorFactory != null ?
                        executorFactory.getExecutor() : null,
                "backup",
                this.sharedStateManager.backupLock(),
                lockRenewPeriodMillis,
                ioCriticalErrorListener);
        this.ioCriticalErrorListener = ioCriticalErrorListener;
        this.sharedStateManager = null;
        this.scheduledLiveLock = null;
        this.scheduledBackupLock = null;
    }

    /**
     * Try to acquire a lock, failing with an exception otherwise.
     */
    private void lock(LeaseLock lock) throws Exception {
        final LeaseLock.AcquireResult acquireResult = lock.tryAcquire(this.lockAcquisitionTimeoutMillis, this.pauser, () -> !this.interrupted);
        switch (acquireResult) {
            case Timeout:
                throw new Exception("timed out waiting for lock");
            case Exit:
                this.interrupted = false;
                throw new InterruptedException("LeaseLock was interrupted");
            case Done:
                break;
            default:
                throw new AssertionError(acquireResult + " not managed");
        }
    }

    private void checkInterrupted(Supplier<String> message) throws InterruptedException {
        if (this.interrupted) {
            interrupted = false;
            throw new InterruptedException(message.get());
        }
    }
    private void renewLiveLockIfNeeded(final long acquiredOn) {
        final long acquiredMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - acquiredOn);
        if (acquiredMillis > this.scheduledLiveLock.renewPeriodMillis()) {
            if (!this.scheduledLiveLock.lock().renew()) {
                final IllegalStateException e = new IllegalStateException("live lock can't be renewed");
                ioCriticalErrorListener.onIOException(e, "live lock can't be renewed", null);
                throw e;
            }
        }
    }

    /**
     * Lock live node and check for a live state, taking care to renew it (if needed) or releasing it otherwise
     */
    private boolean lockLiveAndCheckLiveState() throws Exception {
        lock(this.scheduledLiveLock.lock());
        final long acquiredOn = System.nanoTime();
        boolean liveWhileLocked = false;
        //check if the state is live
        final SharedStateManager.State stateWhileLocked;
        try {
            stateWhileLocked = readSharedState();
        } catch (Throwable t) {
            LOGGER.error("error while holding the live node lock and tried to read the shared state", t);
            this.scheduledLiveLock.lock().release();
            throw t;
        }
        if (stateWhileLocked == SharedStateManager.State.LIVE) {
            renewLiveLockIfNeeded(acquiredOn);
            liveWhileLocked = true;
        } else {
            LOGGER.debug("state is %s while holding the live lock: releasing live lock", stateWhileLocked);
            //state is not live: can (try to) release the lock
            this.scheduledLiveLock.lock().release();
        }
        return liveWhileLocked;
    }

    @Override
    public void awaitLiveNode() throws Exception {
        LOGGER.debug("ENTER awaitLiveNode");
        try {
            boolean liveWhileLocked = false;
            while (!liveWhileLocked) {
                //check first without holding any lock
                final SharedStateManager.State state = readSharedState();
                if (state == SharedStateManager.State.LIVE) {
                    //verify if the state is live while holding the live node lock too
                    liveWhileLocked = lockLiveAndCheckLiveState();
                } else {
                    LOGGER.debug("state while awaiting live node: %s", state);
                }
                if (!liveWhileLocked) {
                    checkInterrupted(() -> "awaitLiveNode got interrupted!");
                    pauser.idle();
                }
            }
            //state is LIVE and live lock is acquired and valid
            LOGGER.debug("acquired live node lock while state is %s: starting scheduledLiveLock", SharedStateManager.State.LIVE);
            this.scheduledLiveLock.start();
        } finally {
            LOGGER.debug("EXIT awaitLiveNode");
        }
    }

    private SharedStateManager.State readSharedState() {
        final SharedStateManager.State state = this.sharedStateManager.readState();
        LOGGER.debug("readSharedState state = %s", state);
        return state;
    }


    @Override
    public void awaitLiveStatus() {
        LOGGER.debug("ENTER awaitLiveStatus");
        try {
            while (readSharedState() != SharedStateManager.State.LIVE) {
                pauser.idle();
            }
        } finally {
            LOGGER.debug("EXIT awaitLiveStatus");
        }
    }


    @Override
    public void startBackup() throws Exception {
        LOGGER.debug("ENTER startBackup");
        try {
            ActiveMQServerLogger.LOGGER.waitingToBecomeBackup();

            lock(scheduledBackupLock.lock());
            scheduledBackupLock.start();
            ActiveMQServerLogger.LOGGER.gotBackupLock();
            if (getUUID() == null)
                readNodeId();
        } finally {
            LOGGER.debug("EXIT startBackup");
        }
    }

    private void writeSharedState(SharedStateManager.State state) {
        LOGGER.debug("writeSharedState state = %s", state);
        this.sharedStateManager.writeState(state);
    }

    private void setFailingBack() {
        writeSharedState(SharedStateManager.State.FAILING_BACK);
    }

    private void setLive() {
        writeSharedState(SharedStateManager.State.LIVE);
    }

    private void setPaused() {
        writeSharedState(SharedStateManager.State.PAUSED);
    }

    @Override
    public ActivateCallback startLiveNode() throws Exception {
        LOGGER.debug("ENTER startLiveNode");
        try {
            setFailingBack();

            final String timeoutMessage = lockAcquisitionTimeoutMillis == -1 ? "indefinitely" : lockAcquisitionTimeoutMillis + " milliseconds";

            ActiveMQServerLogger.LOGGER.waitingToObtainLiveLock(timeoutMessage);

            lock(this.scheduledLiveLock.lock());

            this.scheduledLiveLock.start();

            ActiveMQServerLogger.LOGGER.obtainedLiveLock();

            return new CleaningActivateCallback() {
                @Override
                public void activationComplete() {
                    LOGGER.debug("ENTER activationComplete");
                    try {
                        //state can be written only if the live renew task is running
                        setLive();
                    } catch (Exception e) {
                        ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                    } finally {
                        LOGGER.debug("EXIT activationComplete");
                    }
                }
            };
        } finally {
            LOGGER.debug("EXIT startLiveNode");
        }
    }

    @Override
    public void pauseLiveServer() throws Exception {
        LOGGER.debug("ENTER pauseLiveServer");
        try {
            if (scheduledLiveLock.isStarted()) {
                LOGGER.debug("scheduledLiveLock is running: set paused shared state, stop it and release live lock");
                setPaused();
                scheduledLiveLock.stop();
                scheduledLiveLock.lock().release();
            } else {
                LOGGER.debug("scheduledLiveLock is not running: try renew live lock");
                if (scheduledLiveLock.lock().renew()) {
                    LOGGER.debug("live lock renewed: set paused shared state and release live lock");
                    setPaused();
                    scheduledLiveLock.lock().release();
                } else {
                    final IllegalStateException e = new IllegalStateException("live lock can't be renewed");
                    ioCriticalErrorListener.onIOException(e, "live lock can't be renewed on pauseLiveServer", null);
                    throw e;
                }
            }
        } finally {
            LOGGER.debug("EXIT pauseLiveServer");
        }
    }

    @Override
    public void crashLiveServer() throws Exception {
        LOGGER.debug("ENTER crashLiveServer");
        try {
            if (this.scheduledLiveLock.isStarted()) {
                LOGGER.debug("scheduledLiveLock is running: request stop it and release live lock");
                this.scheduledLiveLock.stop();
                this.scheduledLiveLock.lock().release();
            } else {
                LOGGER.debug("scheduledLiveLock is not running");
            }
        } finally {
            LOGGER.debug("EXIT crashLiveServer");
        }
    }

    @Override
    public void releaseBackup() throws Exception {
        LOGGER.debug("ENTER releaseBackup");
        try {
            if (this.scheduledBackupLock.isStarted()) {
                LOGGER.debug("scheduledBackupLock is running: stop it and release backup lock");
                this.scheduledBackupLock.stop();
                this.scheduledBackupLock.lock().release();
            } else {
                LOGGER.debug("scheduledBackupLock is not running");
            }
        } finally {
            LOGGER.debug("EXIT releaseBackup");
        }
    }

    @Override
    public SimpleString readNodeId() {
        final UUID nodeId = this.sharedStateManager.readNodeId();
        LOGGER.debug("readNodeId nodeId = %s", nodeId);
        setUUID(nodeId);
        return getNodeId();
    }

    @Override
    public boolean isAwaitingFailback() throws Exception {
        LOGGER.debug("ENTER isAwaitingFailback");
        try {
            return readSharedState() == SharedStateManager.State.FAILING_BACK;
        } finally {
            LOGGER.debug("EXIT isAwaitingFailback");
        }
    }

    @Override
    public boolean isBackupLive() throws Exception {
        LOGGER.debug("ENTER isBackupLive");
        try {
            //is anyone holding the live lock?
            return this.scheduledLiveLock.lock().isHeld();
        } finally {
            LOGGER.debug("EXIT isBackupLive");
        }
    }


    @Override
    public void interrupt() {
        LOGGER.debug("ENTER interrupted");
        //need to be volatile: must be called concurrently to work as expected
        interrupted = true;
        LOGGER.debug("EXIT interrupted");
    }
}