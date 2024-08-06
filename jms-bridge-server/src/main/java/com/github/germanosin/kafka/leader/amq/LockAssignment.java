package com.github.germanosin.kafka.leader.amq;

import com.github.germanosin.kafka.leader.Assignment;
import com.github.germanosin.kafka.leader.MemberIdentity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Jacksonized
public class LockAssignment implements Assignment {
    private int version;
    private short error;
    private MemberIdentity liveLock;
    private MemberIdentity backupLock;
    private String clusterState;
    private String clusterId;
    private String assignedLock;
}
