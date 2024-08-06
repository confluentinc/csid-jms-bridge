package io.confluent.amq.server.kafka.nodemanager;

import com.github.germanosin.kafka.leader.Assignment;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Jacksonized
public class LockAssignmentV2 implements Assignment {
    private int version;
    private short error;
    ClusterState clusterState;
    boolean holdsLiveLock;
    boolean holdsBackupLock;
    private String clusterId;
    private boolean updateRequested;
}