package io.confluent.amq.server.kafka;

import io.confluent.amq.persistence.kafka.journal.KJournalAssignment;
import io.confluent.amq.persistence.kafka.journal.KJournalListener;


import io.confluent.amq.persistence.kafka.journal.KJournalState;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

public class KafkaNodeManager extends NodeManager implements KJournalListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNodeManager.class);
    private volatile boolean isAlive = false;
    private final Semaphore idler = new Semaphore( 0);
    private volatile boolean  hasLock = false;
    private final Semaphore lockIdler = new Semaphore( 0);
    private final String brokerId = java.util.UUID.randomUUID().toString();

    public KafkaNodeManager() {
        super(false, null);
    }
    @Override
    public void awaitLiveNode() throws Exception {
        LOGGER.debug("ENTER awaitLiveStatus");
        while (!isAlive | ! hasLock ){
            Thread.sleep(50);
        }
    }

    @Override
    public void awaitLiveStatus() throws Exception {
        LOGGER.debug("ENTER awaitLiveStatus");
        while (!isAlive){
            Thread.sleep(50);
        }
    }

    @Override
    public void startBackup() throws Exception {
        LOGGER.debug("ENTER startBackup");
        // Look into implementation of scheduledBackupLock
    }

    @Override
    public ActivateCallback startLiveNode() throws Exception {
        LOGGER.debug("ENTER startLiveNode");

        ActivateCallback activateCallback = new ActivateCallback() {
        };
        return activateCallback;
    }

    @Override
    public void pauseLiveServer() throws Exception {
        LOGGER.debug("Paused LiveServer");
        hasLock = false;
        isAlive = false;
    }

    @Override
    public void crashLiveServer() throws Exception {
        LOGGER.debug("Paused LiveServer");
        hasLock = false;
        isAlive = false;
    }

    @Override
    public void releaseBackup() throws Exception {
        LOGGER.debug("Backup Lock Released");
    }

    @Override
    public SimpleString readNodeId() throws ActiveMQIllegalStateException, IOException {
        return SimpleString.toSimpleString(brokerId);
    }

    @Override
    public boolean isAwaitingFailback() throws Exception {
        return false;
    }

    @Override
    public boolean isBackupLive() throws Exception {
        return true;
    }

    @Override
    public void interrupt() {
    //
    }

    @Override
    public void onRevokedAssignment(List<KJournalAssignment> assignmentList) {

    }

    @Override
    public void onNewAssignment(List<KJournalAssignment> assignmentList) {
        assignmentList.stream().anyMatch(a->a.journalName().equals( "bindings") && a.partition() == 0);
    }

    @Override
    public void onStateChange(String journalName, KJournalState oldState, KJournalState newState) {

        if (newState == KJournalState.RUNNING) {
            LOGGER.debug("Node Manager released state");
            idler.release(1);
            isAlive = true;
        } else {
            idler.drainPermits();
            isAlive = false;
        }

    }

}
