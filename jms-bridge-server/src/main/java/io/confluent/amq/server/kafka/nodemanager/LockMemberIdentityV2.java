package io.confluent.amq.server.kafka.nodemanager;

import com.github.germanosin.kafka.leader.MemberIdentity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Jacksonized
public class LockMemberIdentityV2 implements MemberIdentity {
    @Builder.Default
    private Integer version = 0;
    private String id;
    private boolean masterPolicy;
    private String requestClusterState;
    private NodeLocks requestLock;
    private NodeLocks releaseLock;
    ClusterState clusterState;
    @Builder.Default
    private boolean leaderEligibility = true;

    @Override
    public boolean getLeaderEligibility() {
        return leaderEligibility;
    }
}
