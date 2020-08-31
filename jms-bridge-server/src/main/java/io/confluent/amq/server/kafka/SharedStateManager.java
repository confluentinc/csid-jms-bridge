package io.confluent.amq.server.kafka;

import java.util.function.Supplier;

import org.apache.activemq.artemis.utils.UUID;

/**
 * Facade to abstract the operations on the shared state (inter-process and/or inter-thread) necessary to coordinate broker nodes.
 */
interface SharedStateManager extends AutoCloseable {

    enum State {
        LIVE, PAUSED, FAILING_BACK, NOT_STARTED, FIRST_TIME_START
    }

    LeaseLock liveLock();

    LeaseLock backupLock();

    UUID readNodeId();

    void writeNodeId(UUID nodeId);

    /**
     * Purpose of this method is to setup the environment to provide a shared state between live/backup servers.
     * That means:
     * - check if a shared state exist and create it/wait for it if not
     * - check if a nodeId exists and create it if not
     *
     * @param nodeIdFactory used to create the nodeId if needed
     * @return the newly created NodeId or the old one if already present
     * @throws IllegalStateException if not able to setup the NodeId properly
     */
    UUID setup(Supplier<? extends UUID> nodeIdFactory);

    State readState();

    void writeState(State state);

    @Override
    default void close() throws Exception {

    }
}