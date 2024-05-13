package com.github.germanosin.kafka.leader.tasks;

import com.github.germanosin.kafka.leader.Assignment;
import com.github.germanosin.kafka.leader.LockMemberIdentity;
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
    private Set<String> acquiredLocks;
    private String updatedState;
    private boolean backupLive;
}
