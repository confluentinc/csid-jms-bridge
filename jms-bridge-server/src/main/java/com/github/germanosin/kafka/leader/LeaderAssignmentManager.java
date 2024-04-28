package com.github.germanosin.kafka.leader;

import com.github.germanosin.kafka.leader.tasks.TaskAssignment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class LeaderAssignmentManager
        implements AssignmentManager<LeaderAssignment, HostMemberIdentity> {

    private boolean isLeader;

    private CountDownLatch standbyLatch;

    @Override
    public void onAssigned(LeaderAssignment assignment, int generation) {
        if (assignment.isLeader() && !isLeader) {
            //just became leader
            isLeader = true;
            synchronized (this) {
                if (standbyLatch != null) {
                    standbyLatch.countDown();
                }
            }
        }

        if (!assignment.isLeader()) {
            isLeader = false;
        }

    }

    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public void onRevoked(Timer timer) {

    }

    @Override
    public Map<HostMemberIdentity, LeaderAssignment> assign(List<HostMemberIdentity> identities) {
        Map<HostMemberIdentity, LeaderAssignment> assignmentMap = new HashMap<>();
        boolean leaderFound = false;
        for (HostMemberIdentity identity : identities) {
            if (identity.preferredLeader()) {
                assignmentMap.put(identity, LeaderAssignment.builder().isLeader(true).build());
                leaderFound = true;
            } else {
                assignmentMap.put(identity, LeaderAssignment.builder().build());
            }
        }

        if (!leaderFound) {
            assignmentMap.put(identities.get(0), LeaderAssignment.builder().isLeader(true).build());
        }

        return assignmentMap;
    }

    public void awaitLeadership() throws InterruptedException {

        if (isLeader) {
            return;
        }

        synchronized (this) {
            if (standbyLatch == null) {
                standbyLatch = new CountDownLatch(1);
            }
        }
        standbyLatch.await();
    }

    @Override
    public boolean isInitialisation() {
        return false;
    }

    @Override
    public boolean isAlive(Timer timer) {
        return false;
    }

    @Override
    public boolean await(Duration timeout) throws LeaderTimeoutException, InterruptedException {
        return true;
    }

    @Override
    public void close() throws Exception {

    }
}