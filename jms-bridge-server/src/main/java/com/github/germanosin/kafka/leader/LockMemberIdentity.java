package com.github.germanosin.kafka.leader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import java.util.HashMap;
import java.util.HashSet;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Jacksonized
public class LockMemberIdentity implements MemberIdentity {
    @Builder.Default
    private Integer version = 0;
    private String id;
    @Builder.Default
    private boolean leaderEligibility = true;
    private String preferredLock;

    @Override
    public boolean getLeaderEligibility() {
        return leaderEligibility;
    }

}
